/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CrossValidationSplitterFactory {

    private static final Logger LOGGER = LogManager.getLogger(CrossValidationSplitterFactory.class);

    private final Client client;
    private final DataFrameAnalyticsConfig config;
    private final List<String> fieldNames;

    public CrossValidationSplitterFactory(Client client, DataFrameAnalyticsConfig config, List<String> fieldNames) {
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
        this.fieldNames = Objects.requireNonNull(fieldNames);
    }

    public CrossValidationSplitter create() {
        if (config.getAnalysis() instanceof Regression) {
            Regression regression = (Regression) config.getAnalysis();
            return new RandomCrossValidationSplitter(
                fieldNames, regression.getDependentVariable(), regression.getTrainingPercent(), regression.getRandomizeSeed());
        }
        if (config.getAnalysis() instanceof Classification) {
            return createStratifiedSplitter((Classification) config.getAnalysis());
        }
        return (row, incrementTrainingDocs, incrementTestDocs) -> incrementTrainingDocs.run();
    }

    private CrossValidationSplitter createStratifiedSplitter(Classification classification) {
        String aggName = "dependent_variable_terms";
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(config.getDest().getIndex())
            .setSize(0)
            .addAggregation(AggregationBuilders.terms(aggName)
                .field(classification.getDependentVariable())
                .size(Classification.MAX_DEPENDENT_VARIABLE_CARDINALITY));
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
            searchRequestBuilder::get);

        ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
        if (shardFailures != null && shardFailures.length > 0) {
            LOGGER.error("[{}] Dependent variable terms search returned shard failures: {}", config.getId(),
                Arrays.toString(shardFailures));
            throw new ElasticsearchException(ExceptionsHelper.shardFailuresToErrorMsg(config.getId(), shardFailures));
        }
        int unavailableShards = searchResponse.getTotalShards() - searchResponse.getSuccessfulShards();
        if (unavailableShards > 0) {
            throw new ElasticsearchException("[" + config.getId() + "] Dependent variable terms search encountered ["
                + unavailableShards + "] unavailable shards");
        }

        Aggregations aggs = searchResponse.getAggregations();
        Terms terms = aggs.get(aggName);
        Map<String, Long> classCardinalities = new HashMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            classCardinalities.put(String.valueOf(bucket.getKey()), bucket.getDocCount());
        }

        return new StratifiedCrossValidationSplitter(fieldNames, classification.getDependentVariable(), classCardinalities,
            classification.getTrainingPercent(), classification.getRandomizeSeed());
    }
}
