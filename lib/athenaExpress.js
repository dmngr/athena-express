"use strict";

const COST_PER_MB = 0.000004768, //Based on $5/TB
	BYTES_IN_MB = 1048576,
	COST_FOR_10MB = COST_PER_MB * 10;

const csv = require("csvtojson");
const Promise = require("bluebird");
const s3_getObject = Promise.promisify(require('@deliverymanager/util').s3_getObject);

class Error {
  constructor(message) {
    this.message = message;
    this.name = "Error"; // (different names for different built-in error classes)
  }
}

const isCommonAthenaError = (err) => {
	return err === "TooManyRequestsException" ||
		err === "ThrottlingException" ||
		err === "NetworkingError" ||
		err === "UnknownEndpoint" ?
		true :
		false;
};

const validateConstructor = (init) => {
	if (!init)
		throw new TypeError("Config object not present in the constructor");
	try {
		init.s3 ? init.s3 : init.aws.config.credentials.accessKeyId;
		new init.aws.Athena({
			apiVersion: "2017-05-18"
		});
	} catch (e) {
		throw new TypeError(
			"AWS object not present or incorrect in the constructor"
		);
	}
};

const startQueryExecution = (query, config) => {
	console.log("startQueryExecution");
	const QueryString = query.sql || query;

	const params = {
		QueryString,
		ResultConfiguration: {
			OutputLocation: config.s3Bucket
		},
		QueryExecutionContext: {
			Database: query.db || config.db
		}
	};
	return new Promise(function (resolve, reject) {
		const startQueryExecutionRecursively = async function () {

			try {
				console.log("startQueryExecutionRecursively");

				const data = await config.athena
					.startQueryExecution(params)
					.promise();

				resolve(data.QueryExecutionId);

			} catch (err) {

				console.log("Error", err);
				isCommonAthenaError(err.code) ?
					setTimeout(() => {
						startQueryExecutionRecursively();
					}, 2000) :
					reject(err);

			}
		};


		startQueryExecutionRecursively();
	});
};

const checkIfExecutionCompleted = (query, QueryExecutionId, config) => {
	console.log("checkIfExecutionCompleted");
	let retries = 0;
	let retry = config.retry;
	return new Promise(function (resolve, reject) {
		const keepCheckingRecursively = async function () {
			try {
				console.log("retries", retries);
				console.log("config.startQueryExecutionRecursivelyCounter", config.startQueryExecutionRecursivelyCounter);
				if (retries > 3 && config.startQueryExecutionRecursivelyCounter < 2) {
					const stopQueryExecution = await config.athena
						.stopQueryExecution({
							QueryExecutionId: QueryExecutionId
						}).promise();
					console.log("stopQueryExecution", stopQueryExecution);

					QueryExecutionId = await startQueryExecution(query, config);
					config.startQueryExecutionRecursivelyCounter++;
					console.log("QueryExecutionId", QueryExecutionId);
					retries = 0;
				} else if (config.startQueryExecutionRecursivelyCounter === 2) {
					return reject(JSON.stringify({
						maxRetries: true
					}));
				}

				const data = await config.athena
					.getQueryExecution({
						QueryExecutionId
					})
					.promise();

				console.log("getQueryExecution", JSON.stringify(data));
				console.log("State", data.QueryExecution.Status.State);
				if (data.QueryExecution.Status.State === "SUCCEEDED") {
					retry = config.retry;
					resolve(data);
				} else if (data.QueryExecution.Status.State === "FAILED") {
					reject(data.QueryExecution.Status.StateChangeReason);
				} else {
					retry = 2000;
					setTimeout(() => {
						retries++;
						keepCheckingRecursively();
					}, retry);
				}
			} catch (err) {
				if (isCommonAthenaError(err.code)) {
					retry = 2000;
					setTimeout(() => {
						keepCheckingRecursively();
					}, retry);
				} else reject(err);
			}
		};
		keepCheckingRecursively();
	});
};

const getQueryResultsFromS3 = async (params) => {

	console.log("getQueryResultsFromS3");

	const bucket = params.config.s3Bucket.replace("s3://", "").split("/")[0];
	const s3Params = {
		Bucket: bucket,
		Key: params.s3Output.replace("s3://" + bucket + "/", "")
	};

	const input = await s3_getObject(s3Params, {
		timeout: 1000,
		expiration_timeout: 1500
	});

	//console.log("input", input.toString('utf8'));
	//console.log("params", JSON.stringify(params));

	if (params.config.formatJson) {
		return await csv().fromString(input.toString('utf8'));
	} else {
		return input;
	}
};

module.exports = class AthenaExpress {
	constructor(init) {
		validateConstructor(init);
		this.config = {
			athena: new init.aws.Athena({
				apiVersion: "2017-05-18"
			}),
			startQueryExecutionRecursivelyCounter: 0,
			s3: new init.aws.S3({
				apiVersion: "2006-03-01"
			}),
			s3Bucket: init.s3 ||
				`s3://athena-express-${init.aws.config.credentials.accessKeyId
					.substring(0, 10)
					.toLowerCase()}-${new Date().getFullYear()}`,
			db: init.db || "default",
			retry: Number(init.retry) || 200,
			formatJson: init.formatJson === false ? false : true,
			getStats: init.getStats
		};
	}

	async query(query) {
		const config = this.config;
		const results = {};

		if (!config)
			throw new TypeError("Config object not present in the constructor");

		if (!query) throw new TypeError("SQL query is missing");

		try {
			const queryExecutionId = await startQueryExecution(query, config);
			console.log("query(ExecutionId", queryExecutionId);
			const queryStatus = await checkIfExecutionCompleted(
				query,
				queryExecutionId,
				config
			);
			console.log("queryStatus", JSON.stringify(queryStatus));
			const s3Output =
				queryStatus.QueryExecution.ResultConfiguration
				.OutputLocation,
				statementType = queryStatus.QueryExecution.StatementType || "DML";

			results.Items = await getQueryResultsFromS3({
				s3Output,
				statementType,
				config
			});

			if (config.getStats) {
				const dataInMb = Math.round(
					queryStatus.QueryExecution.Statistics.DataScannedInBytes /
					BYTES_IN_MB
				);

				results.DataScannedInMB = dataInMb;
				results.QueryCostInUSD =
					dataInMb > 10 ? dataInMb * COST_PER_MB : COST_FOR_10MB;
				results.EngineExecutionTimeInMillis =
					queryStatus.QueryExecution.Statistics.EngineExecutionTimeInMillis;
				results.Count = results.Items.length;
			}

			return results;
		} catch (error) {
			throw new Error(error);
		}
	}
};