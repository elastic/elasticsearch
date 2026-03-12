/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 0, numClientNodes = 0)
public class HttpStatsIT extends HttpSmokeTestCase {

    @SuppressWarnings("unchecked")
    public void testNodeHttpStats() throws IOException {
        internalCluster().startNode();
        performHttpRequests();

        final Response response = getRestClient().performRequest(new Request("GET", "/_nodes/stats/http"));
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            false
        );
        final Map<String, Object> nodesMap = (Map<String, Object>) responseMap.get("nodes");

        assertThat(nodesMap, aMapWithSize(1));
        final String nodeId = nodesMap.keySet().iterator().next();

        assertHttpStats(new XContentTestUtils.JsonMapView((Map<String, Object>) nodesMap.get(nodeId)));
    }

    public void testClusterInfoHttpStats() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(3);
        performHttpRequests();

        final Response response = getRestClient().performRequest(new Request("GET", "/_info/http"));
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            false
        );
        assertHttpStats(new XContentTestUtils.JsonMapView(responseMap));
    }

    private void performHttpRequests() throws IOException {
        // basic request
        final RestClient restClient = getRestClient();
        assertOK(restClient.performRequest(new Request("GET", "/")));
        // request with body and URL placeholder
        final Request searchRequest = new Request("GET", "*/_search");
        searchRequest.setJsonEntity("""
            {"query":{"match_all":{}}}""");
        assertOK(restClient.performRequest(searchRequest));
        // chunked response
        assertOK(restClient.performRequest(new Request("GET", "/_cluster/state")));
        // chunked text response
        assertOK(restClient.performRequest(new Request("GET", "/_cat/nodes")));
    }

    private void assertHttpStats(XContentTestUtils.JsonMapView jsonMapView) {
        final List<String> routes = List.of("/", "/_cat/nodes", "/{index}/_search", "/_cluster/state");

        for (var route : routes) {
            assertThat(route, jsonMapView.get("http.routes." + route), notNullValue());
            assertThat(route, jsonMapView.get("http.routes." + route + ".requests.count"), equalTo(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".requests.total_size_in_bytes"), greaterThanOrEqualTo(0));
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.count"), equalTo(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.total_size_in_bytes"), greaterThan(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".requests.size_histogram"), hasSize(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".requests.size_histogram.0.count"), equalTo(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".requests.size_histogram.0.lt_bytes"), notNullValue());
            if (route.equals("/{index}/_search")) {
                assertThat(route, jsonMapView.get("http.routes." + route + ".requests.size_histogram.0.ge_bytes"), notNullValue());
            }
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.size_histogram"), hasSize(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.size_histogram.0.count"), equalTo(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.size_histogram.0.lt_bytes"), notNullValue());
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.size_histogram.0.ge_bytes"), notNullValue());
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.handling_time_histogram"), hasSize(1));
            assertThat(route, jsonMapView.get("http.routes." + route + ".responses.handling_time_histogram.0.count"), equalTo(1));
            final int ltMillis = jsonMapView.get("http.routes." + route + ".responses.handling_time_histogram.0.lt_millis");
            assertThat(route, ltMillis, notNullValue());
            assertThat(
                route,
                jsonMapView.get("http.routes." + route + ".responses.handling_time_histogram.0.ge_millis"),
                ltMillis > 1 ? notNullValue() : nullValue()
            );
        }
    }
}
