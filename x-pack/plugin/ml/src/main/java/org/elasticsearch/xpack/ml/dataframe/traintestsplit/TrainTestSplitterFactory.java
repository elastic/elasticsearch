/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.traintestsplit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TrainTestSplitterFactory {

    private static final Logger LOGGER = LogManager.getLogger(TrainTestSplitterFactory.class);

    private final Client client;
    private final DataFrameAnalyticsConfig config;
    private final List<String> fieldNames;

    public TrainTestSplitterFactory(Client client, DataFrameAnalyticsConfig config, List<String> fieldNames) {
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
        this.fieldNames = Objects.requireNonNull(fieldNames);
    }

    public TrainTestSplitter create() {
        if (config.getAnalysis() instanceof Regression) {
            return createSingleClassSplitter((Regression) config.getAnalysis());
        }
        if (config.getAnalysis() instanceof Classification) {
            return createStratifiedSplitter((Classification) config.getAnalysis());
        }
        return row -> true;
    }

    private TrainTestSplitter createSingleClassSplitter(Regression regression) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(config.getDest().getIndex())
            .setSize(0)
            .setAllowPartialSearchResults(false)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.existsQuery(regression.getDependentVariable()));

        try {
            SearchResponse searchResponse = ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
                searchRequestBuilder::get);
            return new SingleClassReservoirTrainTestSplitter(fieldNames, regression.getDependentVariable(),
                regression.getTrainingPercent(), regression.getRandomizeSeed(), searchResponse.getHits().getTotalHits().value);
        } catch (Exception e) {
            ParameterizedMessage msg = new ParameterizedMessage("[{}] Error searching total number of training docs", config.getId());
            LOGGER.error(msg, e);
            throw new ElasticsearchException(msg.getFormattedMessage(), e);
        }
    }

    private TrainTestSplitter createStratifiedSplitter(Classification classification) {
        String aggName = "dependent_variable_terms";
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(config.getDest().getIndex())
            .setSize(0)
            .setAllowPartialSearchResults(false)
            .addAggregation(AggregationBuilders.terms(aggName)
                .field(classification.getDependentVariable())
                .size(Classification.MAX_DEPENDENT_VARIABLE_CARDINALITY));

        try {
            SearchResponse searchResponse = ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
                searchRequestBuilder::get);
            Aggregations aggs = searchResponse.getAggregations();
            Terms terms = aggs.get(aggName);
            Map<String, Long> classCounts = new HashMap<>();
            for (Terms.Bucket bucket : terms.getBuckets()) {
                classCounts.put(String.valueOf(bucket.getKey()), bucket.getDocCount());
            }

            return new StratifiedTrainTestSplitter(fieldNames, classification.getDependentVariable(), classCounts,
                classification.getTrainingPercent(), classification.getRandomizeSeed());
        } catch (Exception e) {
            ParameterizedMessage msg = new ParameterizedMessage("[{}] Dependent variable terms search failed", config.getId());
            LOGGER.error(msg, e);
            throw new ElasticsearchException(msg.getFormattedMessage(), e);
        }
    }
}
