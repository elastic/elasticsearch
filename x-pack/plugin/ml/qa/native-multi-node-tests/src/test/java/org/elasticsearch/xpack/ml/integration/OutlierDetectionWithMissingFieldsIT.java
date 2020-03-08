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
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.junit.After;

import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class OutlierDetectionWithMissingFieldsIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testMissingFields() throws Exception {
        String sourceIndex = "test-outlier-detection-with-missing-fields";

        client().admin().indices().prepareCreate(sourceIndex)
            .setMapping("numeric", "type=double", "categorical", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        // 5 docs with valid numeric value and missing categorical field (which should be ignored as it's not analyzed)
        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);
            indexRequest.source("numeric", 42.0);
            bulkRequestBuilder.add(indexRequest);
        }

        // Add a doc with missing field
        {
            IndexRequest missingIndexRequest = new IndexRequest(sourceIndex);
            missingIndexRequest.source("categorical", "foo");
            bulkRequestBuilder.add(missingIndexRequest);
        }

        // Add a doc with numeric being array which is also treated as missing
        {
            IndexRequest arrayIndexRequest = new IndexRequest(sourceIndex);
            arrayIndexRequest.source("numeric", new double[]{1.0, 2.0}, "categorical", "foo");
            bulkRequestBuilder.add(arrayIndexRequest);
        }

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        String id = "test_outlier_detection_with_missing_fields";
        DataFrameAnalyticsConfig config = buildAnalytics(id, sourceIndex, sourceIndex + "-results", null,
            new OutlierDetection.Builder().build());
        registerAnalytics(config);
        putAnalytics(config);

        assertIsStopped(id);
        assertProgress(id, 0, 0, 0, 0);

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
            if (destDoc.containsKey("numeric") && destDoc.get("numeric") instanceof Double) {
                assertThat(destDoc.containsKey("ml"), is(true));
                @SuppressWarnings("unchecked")
                Map<String, Object> resultsObject = (Map<String, Object>) destDoc.get("ml");

                assertThat(resultsObject.containsKey("outlier_score"), is(true));
                double outlierScore = (double) resultsObject.get("outlier_score");
                assertThat(outlierScore, allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(1.0)));
            } else {
                assertThat(destDoc.containsKey("ml"), is(false));
            }
        }

        assertProgress(id, 100, 100, 100, 100);
        assertThat(searchStoredProgress(id).getHits().getTotalHits().value, equalTo(1L));
    }
}
