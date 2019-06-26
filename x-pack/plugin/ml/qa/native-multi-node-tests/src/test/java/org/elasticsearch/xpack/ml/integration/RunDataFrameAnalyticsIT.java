/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.junit.After;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RunDataFrameAnalyticsIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testOutlierDetectionWithFewDocuments() throws Exception {
        String sourceIndex = "test-outlier-detection-with-few-docs";

        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", "numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0, "numeric_2", 1.0, "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_few_docs";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex, null);
        registerAnalytics(config);
        putAnalytics(config);

        assertState(id, DataFrameAnalyticsState.STOPPED);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).get();
        double scoreOfOutlier = 0.0;
        double scoreOfNonOutlier = -1.0;
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
            Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");
            assertThat(resultsObject.containsKey("outlier_score"), is(true));
            double outlierScore = (double) resultsObject.get("outlier_score");
            assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0)));
            if (hit.getId().equals("outlier")) {
                scoreOfOutlier = outlierScore;
            } else {
                if (scoreOfNonOutlier < 0) {
                    scoreOfNonOutlier = outlierScore;
                } else {
                    assertThat(outlierScore, equalTo(scoreOfNonOutlier));
                }
            }
        }
        assertThat(scoreOfOutlier, is(greaterThan(scoreOfNonOutlier)));
    }

    public void testOutlierDetectionWithEnoughDocumentsToScroll() throws Exception {
        String sourceIndex = "test-outlier-detection-with-enough-docs-to-scroll";

        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", "numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        int docCount = randomIntBetween(1024, 2048);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("numeric_1", randomDouble(), "numeric_2", randomFloat(), "categorical_1", randomAlphaOfLength(10));
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_enough_docs_to_scroll";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex, "custom_ml");
        registerAnalytics(config);
        putAnalytics(config);

        assertState(id, DataFrameAnalyticsState.STOPPED);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        // Check we've got all docs
        SearchResponse searchResponse = client().prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) docCount));

        // Check they all have an outlier_score
        searchResponse = client().prepareSearch(config.getDest().getIndex())
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.existsQuery("custom_ml.outlier_score")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) docCount));
    }

    public void testOutlierDetectionWithMoreFieldsThanDocValueFieldLimit() throws Exception {
        String sourceIndex = "test-outlier-detection-with-more-fields-than-docvalue-limit";

        client().admin().indices().prepareCreate(sourceIndex).get();

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
        getSettingsRequest.indices(sourceIndex);
        getSettingsRequest.names(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey());
        getSettingsRequest.includeDefaults(true);

        GetSettingsResponse docValueLimitSetting = client().admin().indices().getSettings(getSettingsRequest).actionGet();
        int docValueLimit = IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(
            docValueLimitSetting.getIndexToSettings().values().iterator().next().value);

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 100; i++) {

            StringBuilder source = new StringBuilder("{");
            for (int fieldCount = 0; fieldCount < docValueLimit + 1; fieldCount++) {
                source.append("\"field_").append(fieldCount).append("\":").append(randomDouble());
                if (fieldCount < docValueLimit) {
                    source.append(",");
                }
            }
            source.append("}");

            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source(source.toString(), XContentType.JSON);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_more_fields_than_docvalue_limit";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex, null);
        registerAnalytics(config);
        putAnalytics(config);

        assertState(id, DataFrameAnalyticsState.STOPPED);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        SearchResponse sourceData = client().prepareSearch(sourceIndex).get();
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
            Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");
            assertThat(resultsObject.containsKey("outlier_score"), is(true));
            double outlierScore = (double) resultsObject.get("outlier_score");
            assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0)));
        }
    }

    public void testStopOutlierDetectionWithEnoughDocumentsToScroll() {
        String sourceIndex = "test-outlier-detection-with-enough-docs-to-scroll";

        client().admin().indices().prepareCreate(sourceIndex)
            .addMapping("_doc", "numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        int docCount = randomIntBetween(1024, 2048);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("numeric_1", randomDouble(), "numeric_2", randomFloat(), "categorical_1", randomAlphaOfLength(10));
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_enough_docs_to_scroll";
        DataFrameAnalyticsConfig config = buildOutlierDetectionAnalytics(id, sourceIndex, "custom_ml");
        registerAnalytics(config);
        putAnalytics(config);

        assertState(id, DataFrameAnalyticsState.STOPPED);
        startAnalytics(id);
        assertState(id, DataFrameAnalyticsState.STARTED);

        assertThat(stopAnalytics(id).isStopped(), is(true));
        assertState(id, DataFrameAnalyticsState.STOPPED);
        if (indexExists(config.getDest().getIndex()) == false) {
            // We stopped before we even created the destination index
            return;
        }

        SearchResponse searchResponse = client().prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).get();
        if (searchResponse.getHits().getTotalHits().value == docCount) {
            searchResponse = client().prepareSearch(config.getDest().getIndex())
                .setTrackTotalHits(true)
                .setQuery(QueryBuilders.existsQuery("custom_ml.outlier_score")).get();
            logger.debug("We stopped during analysis: [{}] < [{}]", searchResponse.getHits().getTotalHits().value, docCount);
            assertThat(searchResponse.getHits().getTotalHits().value, lessThan((long) docCount));
        } else {
            logger.debug("We stopped during reindexing: [{}] < [{}]", searchResponse.getHits().getTotalHits().value, docCount);
        }
    }

    private static DataFrameAnalyticsConfig buildOutlierDetectionAnalytics(String id, String sourceIndex, @Nullable String resultsField) {
        DataFrameAnalyticsConfig.Builder configBuilder = new DataFrameAnalyticsConfig.Builder(id);
        configBuilder.setSource(new DataFrameAnalyticsSource(sourceIndex, null));
        configBuilder.setDest(new DataFrameAnalyticsDest(sourceIndex + "-results", resultsField));
        configBuilder.setAnalysis(new OutlierDetection());
        return configBuilder.build();
    }

    private void assertState(String id, DataFrameAnalyticsState state) {
        List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = getAnalyticsStats(id);
        assertThat(stats.size(), equalTo(1));
        assertThat(stats.get(0).getId(), equalTo(id));
        assertThat(stats.get(0).getState(), equalTo(state));
    }
}
