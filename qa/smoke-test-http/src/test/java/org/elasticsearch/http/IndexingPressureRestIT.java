/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Test Indexing Pressure Metrics and Statistics
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class IndexingPressureRestIT extends HttpSmokeTestCase {

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "1KB")
            .put(unboundedWriteQueue)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testIndexingPressureStats() throws IOException {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "write.wait_for_active_shards": 2
                }
              }
            }""");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request successfulIndexingRequest = new Request("POST", "/index_name/_doc/");
        successfulIndexingRequest.setJsonEntity("{\"x\": \"small text\"}");
        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(CREATED.getStatus()));

        Request getNodeStats = new Request("GET", "/_nodes/stats/indexing_pressure");
        final Response nodeStats = getRestClient().performRequest(getNodeStats);
        Map<String, Object> nodeStatsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, nodeStats.getEntity().getContent(), true);
        ArrayList<Object> values = new ArrayList<>(((Map<Object, Object>) nodeStatsMap.get("nodes")).values());
        assertThat(values.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1 = new XContentTestUtils.JsonMapView((Map<String, Object>) values.get(0));
        Integer node1CombinedBytes = node1.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node1PrimaryBytes = node1.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node1ReplicaBytes = node1.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node1CoordinatingRejections = node1.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node1PrimaryRejections = node1.get("indexing_pressure.memory.total.primary_rejections");
        XContentTestUtils.JsonMapView node2 = new XContentTestUtils.JsonMapView((Map<String, Object>) values.get(1));
        Integer node2IndexingBytes = node2.get("indexing_pressure.memory.total.combined_coordinating_and_primary_in_bytes");
        Integer node2PrimaryBytes = node2.get("indexing_pressure.memory.total.primary_in_bytes");
        Integer node2ReplicaBytes = node2.get("indexing_pressure.memory.total.replica_in_bytes");
        Integer node2CoordinatingRejections = node2.get("indexing_pressure.memory.total.coordinating_rejections");
        Integer node2PrimaryRejections = node2.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CombinedBytes == 0) {
            assertThat(node2IndexingBytes, greaterThan(0));
            assertThat(node2IndexingBytes, lessThan(1024));
        } else {
            assertThat(node1CombinedBytes, greaterThan(0));
            assertThat(node1CombinedBytes, lessThan(1024));
        }

        if (node1ReplicaBytes == 0) {
            assertThat(node1PrimaryBytes, greaterThan(0));
            assertThat(node1PrimaryBytes, lessThan(1024));

            assertThat(node2ReplicaBytes, greaterThan(0));
            assertThat(node2ReplicaBytes, lessThan(1024));
        } else {
            assertThat(node2PrimaryBytes, greaterThan(0));
            assertThat(node2PrimaryBytes, lessThan(1024));

            assertThat(node2ReplicaBytes, equalTo(0));
            assertThat(node1ReplicaBytes, lessThan(1024));
        }

        assertThat(node1CoordinatingRejections, equalTo(0));
        assertThat(node1PrimaryRejections, equalTo(0));
        assertThat(node2CoordinatingRejections, equalTo(0));
        assertThat(node2PrimaryRejections, equalTo(0));

        Request failedIndexingRequest = new Request("POST", "/index_name/_doc/");
        String largeString = randomAlphaOfLength(10000);
        failedIndexingRequest.setJsonEntity("{\"x\": " + largeString + "}");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(failedIndexingRequest));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(TOO_MANY_REQUESTS.getStatus()));

        Request getNodeStats2 = new Request("GET", "/_nodes/stats/indexing_pressure");
        final Response nodeStats2 = getRestClient().performRequest(getNodeStats2);
        Map<String, Object> nodeStatsMap2 = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            nodeStats2.getEntity().getContent(),
            true
        );
        ArrayList<Object> values2 = new ArrayList<>(((Map<Object, Object>) nodeStatsMap2.get("nodes")).values());
        assertThat(values2.size(), equalTo(2));
        XContentTestUtils.JsonMapView node1AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(0));
        node1CoordinatingRejections = node1AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        node1PrimaryRejections = node1.get("indexing_pressure.memory.total.primary_rejections");
        XContentTestUtils.JsonMapView node2AfterRejection = new XContentTestUtils.JsonMapView((Map<String, Object>) values2.get(1));
        node2CoordinatingRejections = node2AfterRejection.get("indexing_pressure.memory.total.coordinating_rejections");
        node2PrimaryRejections = node2AfterRejection.get("indexing_pressure.memory.total.primary_rejections");

        if (node1CoordinatingRejections == 0) {
            assertThat(node2CoordinatingRejections, equalTo(1));
        } else {
            assertThat(node1CoordinatingRejections, equalTo(1));
        }

        assertThat(node1PrimaryRejections, equalTo(0));
        assertThat(node2PrimaryRejections, equalTo(0));
    }
}
