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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class HttpStatsIT extends HttpSmokeTestCase {

    @SuppressWarnings("unchecked")
    public void testHttpStats() throws IOException {
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

        final Response response = restClient.performRequest(new Request("GET", "/_nodes/stats/http"));
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            false
        );
        final Map<String, Object> nodesMap = (Map<String, Object>) responseMap.get("nodes");

        assertThat(nodesMap, aMapWithSize(1));
        final String nodeId = nodesMap.keySet().iterator().next();
        final XContentTestUtils.JsonMapView nodeView = new XContentTestUtils.JsonMapView((Map<String, Object>) nodesMap.get(nodeId));

        final List<String> routes = List.of("/", "/_cat/nodes", "/{index}/_search", "/_cluster/state");

        for (var route : routes) {
            assertThat(nodeView.get("http.routes." + route), notNullValue());
            assertThat(nodeView.get("http.routes." + route + ".requests.count"), equalTo(1));
            assertThat(nodeView.get("http.routes." + route + ".requests.total_size_in_bytes"), greaterThanOrEqualTo(0));
            assertThat(nodeView.get("http.routes." + route + ".responses.count"), equalTo(1));
            assertThat(nodeView.get("http.routes." + route + ".responses.total_size_in_bytes"), greaterThan(1));
            assertThat(nodeView.get("http.routes." + route + ".requests.size_histogram"), hasSize(1));
            assertThat(nodeView.get("http.routes." + route + ".requests.size_histogram.0.count"), equalTo(1));
            assertThat(nodeView.get("http.routes." + route + ".requests.size_histogram.0.lt_bytes"), notNullValue());
            if (route.equals("/{index}/_search")) {
                assertThat(nodeView.get("http.routes." + route + ".requests.size_histogram.0.ge_bytes"), notNullValue());
            }
            assertThat(nodeView.get("http.routes." + route + ".responses.size_histogram"), hasSize(1));
            assertThat(nodeView.get("http.routes." + route + ".responses.size_histogram.0.count"), equalTo(1));
            assertThat(nodeView.get("http.routes." + route + ".responses.size_histogram.0.lt_bytes"), notNullValue());
            assertThat(nodeView.get("http.routes." + route + ".responses.size_histogram.0.ge_bytes"), notNullValue());
            assertThat(nodeView.get("http.routes." + route + ".responses.handling_time_histogram"), hasSize(1));
            assertThat(nodeView.get("http.routes." + route + ".responses.handling_time_histogram.0.count"), equalTo(1));
            assertThat(nodeView.get("http.routes." + route + ".responses.handling_time_histogram.0.lt_millis"), notNullValue());
            assertThat(nodeView.get("http.routes." + route + ".responses.handling_time_histogram.0.ge_millis"), notNullValue());
        }
    }
}
