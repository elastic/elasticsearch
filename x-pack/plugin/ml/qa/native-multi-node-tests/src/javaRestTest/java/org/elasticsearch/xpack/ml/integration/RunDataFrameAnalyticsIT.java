/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.junit.After;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RunDataFrameAnalyticsIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testOutlierDetectionWithFewDocuments() throws Exception {
        String sourceIndex = "test-outlier-detection-with-few-docs";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=unsigned_long", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0, "numeric_2", 1, "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_few_docs";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getDataCounts().getJobId(), equalTo(id));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(5L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertResponse(prepareSearch(sourceIndex), sourceData -> {
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

                @SuppressWarnings("unchecked")
                Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
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
        });

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-outlier-detection-with-few-docs-results]",
            "Started reindexing to destination index [test-outlier-detection-with-few-docs-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-few-docs-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testPreview() throws Exception {
        String sourceIndex = "test-outlier-detection-preview";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=unsigned_long", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric_1", i == 0 ? 100.0 : 1.0, "numeric_2", 1, "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_preview";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);
        List<Map<String, Object>> preview = previewDataFrame(id).getFeatureValues();
        for (Map<String, Object> feature : preview) {
            assertThat(feature.keySet(), hasItems("numeric_1", "numeric_2"));
            assertThat(feature, not(hasKey("categorical_1")));
        }
    }

    public void testOutlierDetectionWithEnoughDocumentsToScroll() throws Exception {
        String sourceIndex = "test-outlier-detection-with-enough-docs-to-scroll";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
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
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            "custom_ml",
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        // Check we've got all docs
        assertHitCount(prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true), docCount);

        // Check they all have an outlier_score
        assertHitCount(
            prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true)
                .setQuery(QueryBuilders.existsQuery("custom_ml.outlier_score")),
            docCount
        );

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-outlier-detection-with-enough-docs-to-scroll-results]",
            "Started reindexing to destination index [test-outlier-detection-with-enough-docs-to-scroll-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-enough-docs-to-scroll-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testOutlierDetectionWithMoreFieldsThanDocValueFieldLimit() throws Exception {
        String sourceIndex = "test-outlier-detection-with-more-fields-than-docvalue-limit";

        client().admin().indices().prepareCreate(sourceIndex).get();

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest(TEST_REQUEST_TIMEOUT);
        getSettingsRequest.indices(sourceIndex);
        getSettingsRequest.names(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey());
        getSettingsRequest.includeDefaults(true);

        GetSettingsResponse docValueLimitSetting = client().admin().indices().getSettings(getSettingsRequest).actionGet();
        int docValueLimit = IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(
            docValueLimitSetting.getIndexToSettings().values().iterator().next()
        );

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
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        assertResponse(prepareSearch(sourceIndex), sourceData -> {
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

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
            }
        });

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-outlier-detection-with-more-fields-than-docvalue-limit-results]",
            "Started reindexing to destination index [test-outlier-detection-with-more-fields-than-docvalue-limit-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-more-fields-than-docvalue-limit-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testStopOutlierDetectionWithEnoughDocumentsToScroll() throws Exception {
        String sourceIndex = "test-stop-outlier-detection-with-enough-docs-to-scroll";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
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

        String id = "test_stop_outlier_detection_with_enough_docs_to_scroll";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            "custom_ml",
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        startAnalytics(id);
        // State here could be any of STARTED, REINDEXING or ANALYZING

        assertThat(stopAnalytics(id).isStopped(), is(true));
        assertIsStopped(id);
        if (indexExists(config.getDest().getIndex()) == false) {
            // We stopped before we even created the destination index
            return;
        }

        assertResponse(prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true), searchResponse -> {
            if (searchResponse.getHits().getTotalHits().value() == docCount) {
                long seenCount = SearchResponseUtils.getTotalHitsValue(
                    prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true)
                        .setQuery(QueryBuilders.existsQuery("custom_ml.outlier_score"))
                );
                logger.debug("We stopped during analysis: [{}] < [{}]", seenCount, docCount);
                assertThat(seenCount, lessThan((long) docCount));
            } else {
                logger.debug("We stopped during reindexing: [{}] < [{}]", searchResponse.getHits().getTotalHits().value(), docCount);
            }
        });

        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-stop-outlier-detection-with-enough-docs-to-scroll-results]",
            "Stopped analytics"
        );
    }

    public void testOutlierDetectionWithMultipleSourceIndices() throws Exception {
        String sourceIndex1 = "test-outlier-detection-with-multiple-source-indices-1";
        String sourceIndex2 = "test-outlier-detection-with-multiple-source-indices-2";
        String destIndex = "test-outlier-detection-with-multiple-source-indices-results";
        String[] sourceIndex = new String[] { sourceIndex1, sourceIndex2 };

        indicesAdmin().prepareCreate(sourceIndex1)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        indicesAdmin().prepareCreate(sourceIndex2)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (String index : sourceIndex) {
            for (int i = 0; i < 5; i++) {
                IndexRequest indexRequest = new IndexRequest(index);
                indexRequest.source("numeric_1", randomDouble(), "numeric_2", randomFloat(), "categorical_1", "foo_" + i);
                bulkRequestBuilder.add(indexRequest);
            }
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_multiple_source_indices";
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(new DataFrameAnalyticsSource(sourceIndex, null, null, null))
            .setDest(new DataFrameAnalyticsDest(destIndex, null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .build();
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        // Check we've got all docs
        assertHitCount(prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true), bulkRequestBuilder.numberOfActions());

        // Check they all have an outlier_score
        assertHitCount(
            prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).setQuery(QueryBuilders.existsQuery("ml.outlier_score")),
            bulkRequestBuilder.numberOfActions()
        );

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-outlier-detection-with-multiple-source-indices-results]",
            "Started reindexing to destination index [test-outlier-detection-with-multiple-source-indices-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-multiple-source-indices-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testOutlierDetectionWithPreExistingDestIndex() throws Exception {
        String sourceIndex = "test-outlier-detection-with-pre-existing-dest-index";
        String destIndex = "test-outlier-detection-with-pre-existing-dest-index-results";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        indicesAdmin().prepareCreate(destIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("numeric_1", randomDouble(), "numeric_2", randomFloat(), "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_pre_existing_dest_index";
        DataFrameAnalyticsConfig config = buildAnalytics(id, sourceIndex, destIndex, null, new OutlierDetection.Builder().build());
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        // Check we've got all docs
        assertHitCount(prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true), bulkRequestBuilder.numberOfActions());
        // Check they all have an outlier_score
        assertHitCount(
            prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true).setQuery(QueryBuilders.existsQuery("ml.outlier_score")),
            bulkRequestBuilder.numberOfActions()
        );

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Using existing destination index [test-outlier-detection-with-pre-existing-dest-index-results]",
            "Started reindexing to destination index [test-outlier-detection-with-pre-existing-dest-index-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-pre-existing-dest-index-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testModelMemoryLimitLowerThanEstimatedMemoryUsage() throws Exception {
        String sourceIndex = "test-model-memory-limit";

        indicesAdmin().prepareCreate(sourceIndex).setMapping("col_1", "type=double", "col_2", "type=float", "col_3", "type=keyword").get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10000; i++) {  // This number of rows should make memory usage estimate greater than 1MB
            IndexRequest indexRequest = new IndexRequest(sourceIndex).id("doc_" + i).source("col_1", 1.0, "col_2", 1.0, "col_3", "str");
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_model_memory_limit_lower_than_estimated_memory_usage";
        ByteSizeValue modelMemoryLimit = ByteSizeValue.ofMb(1);
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, null))
            .setDest(new DataFrameAnalyticsDest(sourceIndex + "-results", null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setModelMemoryLimit(modelMemoryLimit)
            .build();

        putAnalytics(config);
        assertIsStopped(id);
        // should not throw
        startAnalytics(id);
        waitUntilAnalyticsIsFailed(id);
        forceStopAnalytics(id);
        waitUntilAnalyticsIsStopped(id);
    }

    public void testLazyAssignmentWithModelMemoryLimitTooHighForAssignment() throws Exception {
        String sourceIndex = "test-lazy-assign-model-memory-limit-too-high";

        indicesAdmin().prepareCreate(sourceIndex).setMapping("col_1", "type=double", "col_2", "type=float", "col_3", "type=keyword").get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexRequest indexRequest = new IndexRequest(sourceIndex).id("doc_1").source("col_1", 1.0, "col_2", 1.0, "col_3", "str");
        bulkRequestBuilder.add(indexRequest);
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_lazy_assign_model_memory_limit_too_high";
        // Assuming a 1TB job will never fit on the test machine - increase this when machines get really big!
        ByteSizeValue modelMemoryLimit = ByteSizeValue.ofTb(1);
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, null))
            .setDest(new DataFrameAnalyticsDest(sourceIndex + "-results", null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .setModelMemoryLimit(modelMemoryLimit)
            .setAllowLazyStart(true)
            .build();

        putAnalytics(config);
        assertIsStopped(id);

        // Due to lazy start being allowed, this should succeed even though no node currently in the cluster is big enough
        NodeAcknowledgedResponse response = startAnalytics(id);
        assertThat(response.getNode(), emptyString());

        // Wait until state is STARTING, there is no node but there is an assignment explanation.
        assertBusy(() -> {
            GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
            assertThat(stats.getState(), equalTo(DataFrameAnalyticsState.STARTING));
            assertThat(stats.getNode(), is(nullValue()));
            assertThat(stats.getAssignmentExplanation(), containsString("persistent task is awaiting node assignment"));
        });
        stopAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Job requires at least [1tb] free memory on a machine learning node to run",
            "Started analytics",
            "Stopped analytics"
        );
    }

    public void testOutlierDetectionStopAndRestart() throws Exception {
        String sourceIndex = "test-outlier-detection-stop-and-restart";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
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

        String id = "test_outlier_detection_stop_and_restart";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            "custom_ml",
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        NodeAcknowledgedResponse response = startAnalytics(id);
        assertThat(response.getNode(), not(emptyString()));

        String phaseToWait = randomFrom("reindexing", "loading_data", "computing_outliers");
        waitUntilSomeProgressHasBeenMadeForPhase(id, phaseToWait);
        stopAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        // Now let's start it again
        try {
            response = startAnalytics(id);
            assertThat(response.getNode(), not(emptyString()));
        } catch (Exception e) {
            if (e.getMessage().equals("Cannot start because the job has already finished")) {
                // That means the job had managed to complete
            } else {
                throw e;
            }
        }

        waitUntilAnalyticsIsStopped(id);

        // Check we've got all docs
        assertHitCount(prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true), docCount);

        // Check they all have an outlier_score
        assertHitCount(
            prepareSearch(config.getDest().getIndex()).setTrackTotalHits(true)
                .setQuery(QueryBuilders.existsQuery("custom_ml.outlier_score")),
            docCount
        );

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
    }

    public void testOutlierDetectionWithCustomParams() throws Exception {
        String sourceIndex = "test-outlier-detection-with-custom-params";

        indicesAdmin().prepareCreate(sourceIndex)
            .setMapping("numeric_1", "type=double", "numeric_2", "type=float", "categorical_1", "type=keyword")
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

        String id = "test_outlier_detection_with_custom_params";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().setNNeighbors(3)
                .setMethod(OutlierDetection.Method.DISTANCE_KNN)
                .setFeatureInfluenceThreshold(0.01)
                .setComputeFeatureInfluence(false)
                .setOutlierFraction(0.04)
                .setStandardizationEnabled(true)
                .build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);

        assertResponse(prepareSearch(sourceIndex), sourceData -> {
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

                @SuppressWarnings("unchecked")
                Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                assertThat(resultsObject.containsKey("feature_influence"), is(false));

                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
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
        });

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [test-outlier-detection-with-custom-params-results]",
            "Started reindexing to destination index [test-outlier-detection-with-custom-params-results]",
            "Finished reindexing to destination index [test-outlier-detection-with-custom-params-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testOutlierDetection_GivenIndexWithRuntimeFields() throws Exception {
        String sourceIndex = "test-outlier-detection-with-index-with-runtime-fields";

        String mappings = """
            {
              "dynamic": false,
              "runtime": {
                "runtime_numeric": {
                  "type": "double",
                  "script": {
                    "source": "emit(params._source.numeric)",
                    "lang": "painless"
                  }
                }
              }
            }""";

        client().admin().indices().prepareCreate(sourceIndex).setMapping(mappings).get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric", i == 0 ? 100.0 : 1.0);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_index_with_runtime_mappings";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getDataCounts().getJobId(), equalTo(id));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(5L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertResponse(prepareSearch(sourceIndex), sourceData -> {
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

                @SuppressWarnings("unchecked")
                Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
                if (hit.getId().equals("outlier")) {
                    scoreOfOutlier = outlierScore;

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> featureInfluence = (List<Map<String, Object>>) resultsObject.get("feature_influence");
                    assertThat(featureInfluence.size(), equalTo(1));
                    assertThat(featureInfluence.get(0).get("feature_name"), equalTo("runtime_numeric"));
                } else {
                    if (scoreOfNonOutlier < 0) {
                        scoreOfNonOutlier = outlierScore;
                    } else {
                        assertThat(outlierScore, equalTo(scoreOfNonOutlier));
                    }
                }
            }
            assertThat(scoreOfOutlier, is(greaterThan(scoreOfNonOutlier)));
        });

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + sourceIndex + "-results]",
            "Started reindexing to destination index [" + sourceIndex + "-results]",
            "Finished reindexing to destination index [" + sourceIndex + "-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testOutlierDetection_GivenSearchRuntimeMappings() throws Exception {
        String sourceIndex = "test-outlier-detection-index-with-search-runtime-fields";

        String mappings = "{\"enabled\": false}";

        client().admin().indices().prepareCreate(sourceIndex).setMapping(mappings).get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = i == 0 ? "outlier" : "normal" + i;
            indexRequest.id(docId);
            indexRequest.source("numeric", i == 0 ? 100.0 : 1.0);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_index_with_search_runtime_fields";

        Map<String, Object> runtimeMappings = new HashMap<>();
        Map<String, Object> numericFieldRuntimeMapping = new HashMap<>();
        numericFieldRuntimeMapping.put("type", "double");
        numericFieldRuntimeMapping.put("script", "emit(params._source.numeric)");
        runtimeMappings.put("runtime_numeric", numericFieldRuntimeMapping);

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, runtimeMappings))
            .setDest(new DataFrameAnalyticsDest(sourceIndex + "-results", null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .build();
        putAnalytics(config);

        assertIsStopped(id);
        assertProgressIsZero(id);

        startAnalytics(id);
        waitUntilAnalyticsIsStopped(id);
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(id);
        assertThat(stats.getDataCounts().getJobId(), equalTo(id));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(5L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertResponse(prepareSearch(sourceIndex), sourceData -> {
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

                @SuppressWarnings("unchecked")
                Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
                if (hit.getId().equals("outlier")) {
                    scoreOfOutlier = outlierScore;

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> featureInfluence = (List<Map<String, Object>>) resultsObject.get("feature_influence");
                    assertThat(featureInfluence.size(), equalTo(1));
                    assertThat(featureInfluence.get(0).get("feature_name"), equalTo("runtime_numeric"));
                } else {
                    if (scoreOfNonOutlier < 0) {
                        scoreOfNonOutlier = outlierScore;
                    } else {
                        assertThat(outlierScore, equalTo(scoreOfNonOutlier));
                    }
                }
            }
            assertThat(scoreOfOutlier, is(greaterThan(scoreOfNonOutlier)));
        });

        assertProgressComplete(id);
        assertStoredProgressHits(id, 1);
        assertThatAuditMessagesMatch(
            id,
            "Created analytics with type [outlier_detection]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + sourceIndex + "-results]",
            "Started reindexing to destination index [" + sourceIndex + "-results]",
            "Finished reindexing to destination index [" + sourceIndex + "-results]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testStart_GivenTimeout_Returns408() throws Exception {
        String sourceIndex = "test-timeout-returns-408-data";

        client().admin().indices().prepareCreate(sourceIndex).setMapping("numeric_1", "type=integer", "numeric_2", "type=integer").get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.id(String.valueOf(i));
            indexRequest.source("numeric_1", randomInt(), "numeric_2", randomInt());
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test-timeout-returns-408";
        DataFrameAnalyticsConfig config = buildAnalytics(
            id,
            sourceIndex,
            sourceIndex + "-results",
            null,
            new OutlierDetection.Builder().build()
        );
        putAnalytics(config);

        StartDataFrameAnalyticsAction.Request request = new StartDataFrameAnalyticsAction.Request(id);
        request.setTimeout(TimeValue.timeValueNanos(1L));
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> client().execute(StartDataFrameAnalyticsAction.INSTANCE, request).actionGet()
        );

        assertThat(e.status(), equalTo(RestStatus.REQUEST_TIMEOUT));
    }

    @Override
    boolean supportsInference() {
        return false;
    }
}
