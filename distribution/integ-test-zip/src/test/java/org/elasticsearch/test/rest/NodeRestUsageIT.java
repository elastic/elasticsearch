/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class NodeRestUsageIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testWithRestUsage() throws IOException {
        // First get the current usage figures
        Response beforeResponse = client().performRequest("GET",
                randomFrom("_nodes/usage", "_nodes/usage/rest_actions", "_nodes/usage/_all"));
        Map<String, Object> beforeResponseBodyMap = entityAsMap(beforeResponse);
        assertThat(beforeResponseBodyMap, notNullValue());
        Map<String, Object> before_nodesMap = (Map<String, Object>) beforeResponseBodyMap.get("_nodes");
        assertThat(before_nodesMap, notNullValue());
        Integer beforeTotal = (Integer) before_nodesMap.get("total");
        Integer beforeSuccessful = (Integer) before_nodesMap.get("successful");
        Integer beforeFailed = (Integer) before_nodesMap.get("failed");
        assertThat(beforeTotal, greaterThan(0));
        assertThat(beforeSuccessful, equalTo(beforeTotal));
        assertThat(beforeFailed, equalTo(0));

        Map<String, Object> beforeNodesMap = (Map<String, Object>) beforeResponseBodyMap.get("nodes");
        assertThat(beforeNodesMap, notNullValue());
        assertThat(beforeNodesMap.size(), equalTo(beforeSuccessful));
        Map<String, Long> beforeCombinedRestUsage = new HashMap<>();
        beforeCombinedRestUsage.put("nodes_usage_action", 0L);
        beforeCombinedRestUsage.put("create_index_action", 0L);
        beforeCombinedRestUsage.put("document_index_action", 0L);
        beforeCombinedRestUsage.put("search_action", 0L);
        beforeCombinedRestUsage.put("refresh_action", 0L);
        beforeCombinedRestUsage.put("cat_indices_action", 0L);
        beforeCombinedRestUsage.put("nodes_info_action", 0L);
        beforeCombinedRestUsage.put("nodes_stats_action", 0L);
        beforeCombinedRestUsage.put("delete_index_action", 0L);
        for (Map.Entry<String, Object> nodeEntry : beforeNodesMap.entrySet()) {
            Map<String, Object> beforeRestActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue())
                    .get("rest_actions");
            assertThat(beforeRestActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : beforeRestActionUsage.entrySet()) {
                Long currentUsage = beforeCombinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    beforeCombinedRestUsage.put(restActionEntry.getKey(), ((Number) restActionEntry.getValue()).longValue());
                } else {
                    beforeCombinedRestUsage.put(restActionEntry.getKey(), currentUsage + ((Number) restActionEntry.getValue()).longValue());
                }
            }
        }

        // Do some requests to get some rest usage stats
        client().performRequest("PUT", "/test");
        client().performRequest("POST", "/test/doc/1", Collections.emptyMap(),
                new StringEntity("{ \"foo\": \"bar\"}", ContentType.APPLICATION_JSON));
        client().performRequest("POST", "/test/doc/2", Collections.emptyMap(),
                new StringEntity("{ \"foo\": \"bar\"}", ContentType.APPLICATION_JSON));
        client().performRequest("POST", "/test/doc/3", Collections.emptyMap(),
                new StringEntity("{ \"foo\": \"bar\"}", ContentType.APPLICATION_JSON));
        client().performRequest("GET", "/test/_search");
        client().performRequest("POST", "/test/doc/4", Collections.emptyMap(),
                new StringEntity("{ \"foo\": \"bar\"}", ContentType.APPLICATION_JSON));
        client().performRequest("POST", "/test/_refresh");
        client().performRequest("GET", "/_cat/indices");
        client().performRequest("GET", "/_nodes");
        client().performRequest("GET", "/test/_search");
        client().performRequest("GET", "/_nodes/stats");
        client().performRequest("DELETE", "/test");

        Response response = client().performRequest("GET", "_nodes/usage");
        Map<String, Object> responseBodyMap = entityAsMap(response);
        assertThat(responseBodyMap, notNullValue());
        Map<String, Object> _nodesMap = (Map<String, Object>) responseBodyMap.get("_nodes");
        assertThat(_nodesMap, notNullValue());
        Integer total = (Integer) _nodesMap.get("total");
        Integer successful = (Integer) _nodesMap.get("successful");
        Integer failed = (Integer) _nodesMap.get("failed");
        assertThat(total, greaterThan(0));
        assertThat(successful, equalTo(total));
        assertThat(failed, equalTo(0));

        Map<String, Object> nodesMap = (Map<String, Object>) responseBodyMap.get("nodes");
        assertThat(nodesMap, notNullValue());
        assertThat(nodesMap.size(), equalTo(successful));
        Map<String, Long> combinedRestUsage = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : restActionUsage.entrySet()) {
                Long currentUsage = combinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    combinedRestUsage.put(restActionEntry.getKey(), ((Number) restActionEntry.getValue()).longValue());
                } else {
                    combinedRestUsage.put(restActionEntry.getKey(), currentUsage + ((Number) restActionEntry.getValue()).longValue());
                }
            }
        }
        assertThat(combinedRestUsage.get("nodes_usage_action") - beforeCombinedRestUsage.get("nodes_usage_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("create_index_action") - beforeCombinedRestUsage.get("create_index_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("document_index_action") - beforeCombinedRestUsage.get("document_index_action"), equalTo(4L));
        assertThat(combinedRestUsage.get("search_action") - beforeCombinedRestUsage.get("search_action"), equalTo(2L));
        assertThat(combinedRestUsage.get("refresh_action") - beforeCombinedRestUsage.get("refresh_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("cat_indices_action") - beforeCombinedRestUsage.get("cat_indices_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("nodes_info_action") - beforeCombinedRestUsage.get("nodes_info_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("nodes_stats_action") - beforeCombinedRestUsage.get("nodes_stats_action"), equalTo(1L));
        assertThat(combinedRestUsage.get("delete_index_action") - beforeCombinedRestUsage.get("delete_index_action"), equalTo(1L));

    }

    public void testMetricsWithAll() throws IOException {
        ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest("GET", "_nodes/usage/_all,rest_actions"));
        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("\"type\":\"illegal_argument_exception\","
                + "\"reason\":\"request [_nodes/usage/_all,rest_actions] contains _all and individual metrics [_all,rest_actions]\""));
    }

}
