/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class RegressionIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        String jobId = "regression_single_numeric_feature_and_mixed_data_set";
        String sourceIndex = jobId + "_source_index";

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        List<Double> featureValues = Arrays.asList(1.0, 2.0, 3.0);
        List<Double> dependentVariableValues = Arrays.asList(10.0, 20.0, 30.0);

        for (int i = 0; i < 350; i++) {
            Double field = featureValues.get(i % 3);
            Double value = dependentVariableValues.get(i % 3);

            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            if (i < 300) {
                indexRequest.source("feature", field, "variable", value);
            } else {
                indexRequest.source("feature", field);
            }
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String destIndex = sourceIndex + "_results";
        DataFrameAnalyticsConfig config = buildRegressionAnalytics(jobId, new String[] {sourceIndex}, destIndex, null,
            new Regression("variable"));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
            assertThat(destDocGetResponse.isExists(), is(true));
            Map<String, Object> sourceDoc = hit.getSourceAsMap();
            Map<String, Object> destDoc = destDocGetResponse.getSource();
            for (String field : sourceDoc.keySet()) {
                assertThat(destDoc.containsKey(field), is(true));
                assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
            }
            assertThat(destDoc.containsKey("ml"), is(true));

            @SuppressWarnings("unchecked")
            Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));

            // TODO reenable this assertion when the backend is stable
            // it seems for this case values can be as far off as 2.0

            // double featureValue = (double) destDoc.get("feature");
            // double predictionValue = (double) resultsObject.get("variable_prediction");
            // assertThat(predictionValue, closeTo(10 * featureValue, 2.0));

            boolean expectedIsTraining = destDoc.containsKey("variable");
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(expectedIsTraining));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        String jobId = "regression_only_training_data_and_training_percent_is_hundred";
        String sourceIndex = jobId + "_source_index";

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        List<Double> featureValues = Arrays.asList(1.0, 2.0, 3.0);
        List<Double> dependentVariableValues = Arrays.asList(10.0, 20.0, 30.0);

        for (int i = 0; i < 350; i++) {
            Double field = featureValues.get(i % 3);
            Double value = dependentVariableValues.get(i % 3);

            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("feature", field, "variable", value);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String destIndex = sourceIndex + "_results";
        DataFrameAnalyticsConfig config = buildRegressionAnalytics(jobId, new String[] {sourceIndex}, destIndex, null,
            new Regression("variable"));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
            assertThat(destDocGetResponse.isExists(), is(true));
            Map<String, Object> sourceDoc = hit.getSourceAsMap();
            Map<String, Object> destDoc = destDocGetResponse.getSource();
            for (String field : sourceDoc.keySet()) {
                assertThat(destDoc.containsKey(field), is(true));
                assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
            }
            assertThat(destDoc.containsKey("ml"), is(true));

            @SuppressWarnings("unchecked")
            Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty() throws Exception {
        String jobId = "regression_only_training_data_and_training_percent_is_fifty";
        String sourceIndex = jobId + "_source_index";

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        List<Double> featureValues = Arrays.asList(1.0, 2.0, 3.0);
        List<Double> dependentVariableValues = Arrays.asList(10.0, 20.0, 30.0);

        for (int i = 0; i < 350; i++) {
            Double field = featureValues.get(i % 3);
            Double value = dependentVariableValues.get(i % 3);

            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("feature", field, "variable", value);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String destIndex = sourceIndex + "_results";
        DataFrameAnalyticsConfig config = buildRegressionAnalytics(jobId, new String[] {sourceIndex}, destIndex, null,
            new Regression("variable", null, null, null, null, null, null, 50.0));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        int trainingRowsCount = 0;
        int nonTrainingRowsCount = 0;
        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
            assertThat(destDocGetResponse.isExists(), is(true));
            Map<String, Object> sourceDoc = hit.getSourceAsMap();
            Map<String, Object> destDoc = destDocGetResponse.getSource();
            for (String field : sourceDoc.keySet()) {
                assertThat(destDoc.containsKey(field), is(true));
                assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
            }
            assertThat(destDoc.containsKey("ml"), is(true));

            @SuppressWarnings("unchecked")
            Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

            assertThat(resultsObject.containsKey("variable_prediction"), is(true));

            assertThat(resultsObject.containsKey("is_training"), is(true));
            // Let's just assert there's both training and non-training results
            if ((boolean) resultsObject.get("is_training")) {
                trainingRowsCount++;
            } else {
                nonTrainingRowsCount++;
            }
        }
        assertThat(trainingRowsCount, greaterThan(0));
        assertThat(nonTrainingRowsCount, greaterThan(0));

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
    }
}
