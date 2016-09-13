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

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.elasticsearch.rest.action.admin.indices.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.RestRefreshAction;
import org.elasticsearch.rest.action.cat.RestIndicesAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NodeRestUsageIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testNoRestUsage() throws IOException {
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
        Map<String, Integer> combinedRestUsage = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : restActionUsage.entrySet()) {
                Integer currentUsage = combinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    combinedRestUsage.put(restActionEntry.getKey(), (Integer) restActionEntry.getValue());
                } else {
                    combinedRestUsage.put(restActionEntry.getKey(), currentUsage + (Integer) restActionEntry.getValue());
                }
            }
        }
        // The call to the nodes usage api counts in the stats so it should be
        // the only thing returned in the response
        assertThat(combinedRestUsage.size(), equalTo(1));
        assertThat(combinedRestUsage.get(RestNodesUsageAction.class.getName()), equalTo(1));

    }

    @SuppressWarnings("unchecked")
    public void testWithRestUsage() throws IOException {
        // Do some requests to get some rest usage stats
        client().performRequest("PUT", "/test");
        client().performRequest("POST", "/test/doc/1", Collections.emptyMap(), new StringEntity("{ \"foo\": \"bar\"}"));
        client().performRequest("POST", "/test/doc/2", Collections.emptyMap(), new StringEntity("{ \"foo\": \"bar\"}"));
        client().performRequest("POST", "/test/doc/3", Collections.emptyMap(), new StringEntity("{ \"foo\": \"bar\"}"));
        client().performRequest("GET", "/test/_search");
        client().performRequest("POST", "/test/doc/4", Collections.emptyMap(), new StringEntity("{ \"foo\": \"bar\"}"));
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
        Map<String, Integer> combinedRestUsage = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : restActionUsage.entrySet()) {
                Integer currentUsage = combinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    combinedRestUsage.put(restActionEntry.getKey(), (Integer) restActionEntry.getValue());
                } else {
                    combinedRestUsage.put(restActionEntry.getKey(), currentUsage + (Integer) restActionEntry.getValue());
                }
            }
        }
        // The call to the nodes usage api counts in the stats so it should be
        // the only thing returned in the response
        assertThat(combinedRestUsage.size(), equalTo(9));
        assertThat(combinedRestUsage.get(RestNodesUsageAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestCreateIndexAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestIndexAction.class.getName()), equalTo(4));
        assertThat(combinedRestUsage.get(RestSearchAction.class.getName()), equalTo(2));
        assertThat(combinedRestUsage.get(RestRefreshAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestIndicesAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestNodesInfoAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestNodesStatsAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestDeleteIndexAction.class.getName()), equalTo(1));

    }

    @SuppressWarnings("unchecked")
    public void testClearRestUsage() throws IOException {
        // Do some requests to get some rest usage stats
        client().performRequest("GET", "/_cat/indices");
        client().performRequest("GET", "/_nodes");
        client().performRequest("GET", "/_nodes/stats");

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
        Map<String, Integer> combinedRestUsage = new HashMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, notNullValue());
            for (Map.Entry<String, Object> restActionEntry : restActionUsage.entrySet()) {
                Integer currentUsage = combinedRestUsage.get(restActionEntry.getKey());
                if (currentUsage == null) {
                    combinedRestUsage.put(restActionEntry.getKey(), (Integer) restActionEntry.getValue());
                } else {
                    combinedRestUsage.put(restActionEntry.getKey(), currentUsage + (Integer) restActionEntry.getValue());
                }
            }
        }
        // The call to the nodes usage api counts in the stats so it should be
        // the only thing returned in the response
        assertThat(combinedRestUsage.size(), equalTo(4));
        assertThat(combinedRestUsage.get(RestNodesUsageAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestIndicesAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestNodesInfoAction.class.getName()), equalTo(1));
        assertThat(combinedRestUsage.get(RestNodesStatsAction.class.getName()), equalTo(1));

        response = client().performRequest("GET", "_nodes/usage/_clear");
        responseBodyMap = entityAsMap(response);
        assertThat(responseBodyMap, notNullValue());
        _nodesMap = (Map<String, Object>) responseBodyMap.get("_nodes");
        assertThat(_nodesMap, notNullValue());
        total = (Integer) _nodesMap.get("total");
        successful = (Integer) _nodesMap.get("successful");
        failed = (Integer) _nodesMap.get("failed");
        assertThat(total, greaterThan(0));
        assertThat(successful, equalTo(total));
        assertThat(failed, equalTo(0));

        nodesMap = (Map<String, Object>) responseBodyMap.get("nodes");
        assertThat(nodesMap, notNullValue());
        assertThat(nodesMap.size(), equalTo(successful));
        for (Map.Entry<String, Object> nodeEntry : nodesMap.entrySet()) {
            Map<String, Object> restActionUsage = (Map<String, Object>) ((Map<String, Object>) nodeEntry.getValue()).get("rest_actions");
            assertThat(restActionUsage, nullValue());
        }
    }

}
