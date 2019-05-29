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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction.ALLOWED_METRICS;

public class RestNodesInfoActionTests extends ESTestCase {

    public void testDuplicatedFiltersAreNotRemoved() {
        Map<String, String> params = new HashMap<>();
        params.put("nodeId", "_all,master:false,_all");
        
        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all", "master:false", "_all" }, actual.nodesIds());
    }
    
    public void testOnlyMetrics() {
        Map<String, String> params = new HashMap<>();
        int metricsCount = randomIntBetween(1, ALLOWED_METRICS.size());
        List<String> metrics = new ArrayList<>();
        
        for(int i = 0; i < metricsCount; i++) {
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        params.put("nodeId", String.join(",", metrics));
        
        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all" }, actual.nodesIds());
        assertMetrics(metrics, actual);
    }
    
    public void testAllMetricsSelectedWhenNodeAndMetricSpecified() {
        Map<String, String> params = new HashMap<>();
        String nodeId = randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23));
        String metric = randomFrom(ALLOWED_METRICS);
        
        params.put("nodeId", nodeId + "," + metric);
        RestRequest restRequest = buildRestRequest(params);
        
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { nodeId, metric }, actual.nodesIds());
        assertAllMetricsTrue(actual);
    }
    
    public void testSeparateNodeIdsAndMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);
        List<String> metrics = new ArrayList<>(5);
        
        for(int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        
        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", String.join(",", metrics));
        RestRequest restRequest = buildRestRequest(params);
        
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertMetrics(metrics, actual);
    }

    public void testExplicitAllMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);

        for(int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
        }
        
        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", "_all");
        RestRequest restRequest = buildRestRequest(params);
        
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertAllMetricsTrue(actual);
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.GET)
                .withPath("/_nodes")
                .withParams(params)
                .build();
    }
    
    private void assertMetrics(List<String> metrics, NodesInfoRequest nodesInfoRequest) {
        assertTrue((metrics.contains("http") && nodesInfoRequest.http()) || metrics.contains("http") == false);
        assertTrue((metrics.contains("ingest") && nodesInfoRequest.ingest()) || metrics.contains("ingest") == false);
        assertTrue((metrics.contains("indices") && nodesInfoRequest.indices()) || metrics.contains("indices") == false);
        assertTrue((metrics.contains("jvm") && nodesInfoRequest.jvm()) || metrics.contains("jvm") == false);
        assertTrue((metrics.contains("os") && nodesInfoRequest.os()) || metrics.contains("os") == false);
        assertTrue((metrics.contains("plugins") && nodesInfoRequest.plugins()) || metrics.contains("plugins") == false);
        assertTrue((metrics.contains("process") && nodesInfoRequest.process()) || metrics.contains("process") == false);
        assertTrue((metrics.contains("settings") && nodesInfoRequest.settings()) || metrics.contains("settings") == false);
        assertTrue((metrics.contains("thread_pool") && nodesInfoRequest.threadPool()) || metrics.contains("thread_pool") == false);
        assertTrue((metrics.contains("transport") && nodesInfoRequest.transport()) || metrics.contains("transport") == false);
    }
    
    private void assertAllMetricsTrue(NodesInfoRequest nodesInfoRequest) {
        assertTrue(nodesInfoRequest.http());
        assertTrue(nodesInfoRequest.ingest());
        assertTrue(nodesInfoRequest.indices());
        assertTrue(nodesInfoRequest.jvm());
        assertTrue(nodesInfoRequest.os());
        assertTrue(nodesInfoRequest.plugins());
        assertTrue(nodesInfoRequest.process());
        assertTrue(nodesInfoRequest.settings());
        assertTrue(nodesInfoRequest.threadPool());
        assertTrue(nodesInfoRequest.transport());
    }
}
