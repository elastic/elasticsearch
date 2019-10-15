/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import com.google.common.collect.Ordering;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParamsTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClassificationIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String NUMERICAL_FIELD = "numerical-field";
    private static final String KEYWORD_FIELD = "keyword-field";
    private static final List<Double> NUMERICAL_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    private static final List<String> KEYWORD_FIELD_VALUES = Collections.unmodifiableList(Arrays.asList("dog", "cat"));

    private String jobId;
    private String sourceIndex;
    private String destIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("classification_single_numeric_feature_and_mixed_data_set");
        indexData(sourceIndex, 300, 50, NUMERICAL_FIELD_VALUES, KEYWORD_FIELD_VALUES);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(KEYWORD_FIELD)));
            assertThat(resultsObject.containsKey("top_classes"), is(false));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("classification_only_training_data_and_training_percent_is_100");
        indexData(sourceIndex, 300, 0, NUMERICAL_FIELD_VALUES, KEYWORD_FIELD_VALUES);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));
            assertThat(resultsObject.containsKey("is_training"), is(true));
            assertThat(resultsObject.get("is_training"), is(true));
            assertThat(resultsObject.containsKey("top_classes"), is(false));
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty() throws Exception {
        initialize("classification_only_training_data_and_training_percent_is_50");
        indexData(sourceIndex, 300, 0, NUMERICAL_FIELD_VALUES, KEYWORD_FIELD_VALUES);

        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Classification(KEYWORD_FIELD, BoostedTreeParamsTests.createRandom(), null, null, 50.0));
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
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));
            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));

            assertThat(resultsObject.containsKey("is_training"), is(true));
            // Let's just assert there's both training and non-training results
            if ((boolean) resultsObject.get("is_training")) {
                trainingRowsCount++;
            } else {
                nonTrainingRowsCount++;
            }
            assertThat(resultsObject.containsKey("top_classes"), is(false));
        }
        assertThat(trainingRowsCount, greaterThan(0));
        assertThat(nonTrainingRowsCount, greaterThan(0));

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows_TopClassesRequested() throws Exception {
        initialize("classification_top_classes_requested");
        indexData(sourceIndex, 300, 0, NUMERICAL_FIELD_VALUES, KEYWORD_FIELD_VALUES);

        int numTopClasses = 2;
        DataFrameAnalyticsConfig config =
            buildAnalytics(
                jobId,
                sourceIndex,
                destIndex,
                null,
                new Classification(KEYWORD_FIELD, BoostedTreeParamsTests.createRandom(), null, numTopClasses, null));
        registerAnalytics(config);
        putAnalytics(config);

        assertState(jobId, DataFrameAnalyticsState.STOPPED);
        assertProgress(jobId, 0, 0, 0, 0);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000).get();
        for (SearchHit hit : sourceData.getHits()) {
            Map<String, Object> destDoc = getDestDoc(config, hit);
            Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

            assertThat(resultsObject.containsKey("keyword-field_prediction"), is(true));
            assertThat((String) resultsObject.get("keyword-field_prediction"), is(in(KEYWORD_FIELD_VALUES)));
            assertTopClasses(resultsObject, numTopClasses);
        }

        assertProgress(jobId, 100, 100, 100, 100);
        assertThat(searchStoredProgress(jobId).getHits().getTotalHits().value, equalTo(1L));
        assertThatAuditMessagesMatch(jobId,
            "Created analytics with analysis type [classification]",
            "Estimated memory usage for this analytics to be",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Finished analysis");
    }

    public void testDependentVariableCardinalityTooHighError() {
        initialize("cardinality_too_high");
        indexData(sourceIndex, 6, 5, NUMERICAL_FIELD_VALUES, Arrays.asList("dog", "cat", "fox"));

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        registerAnalytics(config);
        putAnalytics(config);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> startAnalytics(jobId));
        assertThat(e.status().getStatus(), equalTo(400));
        assertThat(e.getMessage(), equalTo("Field [keyword-field] must have at most [2] distinct values but there were at least [3]"));
    }

    private void initialize(String jobId) {
        this.jobId = jobId;
        this.sourceIndex = jobId + "_source_index";
        this.destIndex = sourceIndex + "_results";
    }

    private static void indexData(String sourceIndex,
                                  int numTrainingRows, int numNonTrainingRows,
                                  List<Double> numericalFieldValues, List<String> keywordFieldValues) {
        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", NUMERICAL_FIELD, "type=double", KEYWORD_FIELD, "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            Double numericalValue = numericalFieldValues.get(i % numericalFieldValues.size());
            String keywordValue = keywordFieldValues.get(i % keywordFieldValues.size());

            IndexRequest indexRequest = new IndexRequest(sourceIndex)
                .source(NUMERICAL_FIELD, numericalValue, KEYWORD_FIELD, keywordValue);
            bulkRequestBuilder.add(indexRequest);
        }
        for (int i = numTrainingRows; i < numTrainingRows + numNonTrainingRows; i++) {
            Double numericalValue = numericalFieldValues.get(i % numericalFieldValues.size());

            IndexRequest indexRequest = new IndexRequest(sourceIndex)
                .source(NUMERICAL_FIELD, numericalValue);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private static Map<String, Object> getDestDoc(DataFrameAnalyticsConfig config, SearchHit hit) {
        GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
        assertThat(destDocGetResponse.isExists(), is(true));
        Map<String, Object> sourceDoc = hit.getSourceAsMap();
        Map<String, Object> destDoc = destDocGetResponse.getSource();
        for (String field : sourceDoc.keySet()) {
            assertThat(destDoc.containsKey(field), is(true));
            assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
        }
        return destDoc;
    }

    private static Map<String, Object> getMlResultsObjectFromDestDoc(Map<String, Object> destDoc) {
        assertThat(destDoc.containsKey("ml"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");
        return resultsObject;
    }

    private static void assertTopClasses(Map<String, Object> resultsObject, int numTopClasses) {
        assertThat(resultsObject.containsKey("top_classes"), is(true));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> topClasses = (List<Map<String, Object>>) resultsObject.get("top_classes");
        assertThat(topClasses, hasSize(numTopClasses));
        List<String> classNames = new ArrayList<>(topClasses.size());
        List<Double> classProbabilities = new ArrayList<>(topClasses.size());
        for (Map<String, Object> topClass : topClasses) {
            assertThat(topClass, allOf(hasKey("class_name"), hasKey("class_probability")));
            classNames.add((String) topClass.get("class_name"));
            classProbabilities.add((Double) topClass.get("class_probability"));
        }
        // Assert that all the predicted class names come from the set of keyword field values.
        classNames.forEach(className -> assertThat(className, is(in(KEYWORD_FIELD_VALUES))));
        // Assert that the first class listed in top classes is the same as the predicted class.
        assertThat(classNames.get(0), equalTo(resultsObject.get("keyword-field_prediction")));
        // Assert that all the class probabilities lie within [0, 1] interval.
        classProbabilities.forEach(p -> assertThat(p, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0))));
        // Assert that the top classes are listed in the order of decreasing probabilities.
        assertThat(Ordering.natural().reverse().isOrdered(classProbabilities), is(true));
    }
}
