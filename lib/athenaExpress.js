"use strict";

const COST_PER_MB = 0.000004768, //Based on $5/TB
	BYTES_IN_MB = 1048576,
	COST_FOR_10MB = COST_PER_MB * 10;

const csv = require("csvtojson");


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
		console.log(init.s3 ? init.s3 : init.aws.config.credentials.accessKeyId);
		console.log(new init.aws.Athena({
			apiVersion: "2017-05-18"
		}));
	} catch (e) {
		throw new TypeError(
			"AWS object not present or incorrect in the constructor"
		);
	}
};

const startQueryExecution = (query, config) => {
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
				const data = await config.athena
					.startQueryExecution(params)
					.promise();
				resolve(data.QueryExecutionId);
			} catch (err) {
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

const checkIfExecutionCompleted = (QueryExecutionId, config) => {
	let retry = config.retry;
	return new Promise(function (resolve, reject) {
		const keepCheckingRecursively = async function () {
			try {
				const data = await config.athena
					.getQueryExecution({
						QueryExecutionId
					})
					.promise();
				if (data.QueryExecution.Status.State === "SUCCEEDED") {
					retry = config.retry;
					resolve(data);
				} else if (data.QueryExecution.Status.State === "FAILED") {
					reject(data.QueryExecution.Status.StateChangeReason);
				} else {
					setTimeout(() => {
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

const getQueryResultsFromS3 = (params) => {
	const bucket = params.config.s3Bucket.replace("s3://", "").split("/")[0];
	const s3Params = {
			Bucket: bucket,
			Key: params.s3Output.replace("s3://" + bucket + "/", "")
		},
		input = params.config.s3.getObject(s3Params).createReadStream();

	console.log("input", input);
	console.log("params", JSON.stringify(params));
	if (params.config.formatJson) {
		return new Promise(function (resolve) {

			input.pipe(csv()).then((jsonObj) => {
				console.log(jsonObj);
				return resolve(jsonObj);
			});
		});
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
			const queryStatus = await checkIfExecutionCompleted(
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