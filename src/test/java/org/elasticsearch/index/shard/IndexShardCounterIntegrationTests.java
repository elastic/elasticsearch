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


package org.elasticsearch.index.shard;

import com.google.common.base.Predicate;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class IndexShardCounterIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .build();
    }

    @Test
    public void testCounterStaysIncrementedWhileReplicaOpsInFlight() throws InterruptedException, ExecutionException {

        final ConcurrentHashMap<String, Map<String, Object>> pendingRequests = new ConcurrentHashMap<>();
        //create a transport service that collects indexing requests. can only collect one request per node
        for (final String nodeName : internalCluster().getNodeNames()) {
            for (final String otherNodeName : internalCluster().getNodeNames()) {
                if (nodeName.equals(otherNodeName)) {
                    continue;
                }
                MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, nodeName));
                mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, otherNodeName).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {
                    @Override
                    public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                        if (action.equals(IndexAction.NAME+ "[r]") && pendingRequests.containsKey(nodeName) == false) {
                            Map<String, Object> requestMap = new HashMap<>();
                            requestMap.put("node", node);
                            requestMap.put("requestId", requestId);
                            requestMap.put("action", action);
                            requestMap.put("request", request);
                            requestMap.put("options", options);
                            pendingRequests.put(nodeName, requestMap);
                        } else {
                            super.sendRequest(node, requestId, action, request, options);
                        }
                    }
                });
            }
        }

        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_shards", 1).put("number_of_replicas", 1)));
        ensureGreen("test");
        final IndexShard primaryIndexShard = getPrimaryIndexShard();
        Future<IndexResponse> indexResponse = client().prepareIndex("test", "doc", "1").setSource("foo", "bar").execute();
        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return pendingRequests.size() == 1;
            }
        }));

        assertTrue(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return primaryIndexShard.getOperationCounter() == 2;
            }
        }));
        assertThat(primaryIndexShard.getOperationCounter(), equalTo(2));
        sendPendingRequests(pendingRequests);
        indexResponse.get();
    }

    void sendPendingRequests(ConcurrentHashMap<String, Map<String, Object>> pendingRequests) {
        for (String nodeName : pendingRequests.keySet()) {
            Map<String, Object> requestMap = pendingRequests.get(nodeName);
            MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            Transport transport = mockTransportService.transport().getTransport((DiscoveryNode) requestMap.get("node"));
            try {
                transport.sendRequest((DiscoveryNode) requestMap.get("node"), (long) requestMap.get("requestId"), (String) requestMap.get("action"), (TransportRequest) requestMap.get("request"), (TransportRequestOptions) requestMap.get("options"));
            } catch (IOException e) {
            }
        }
    }

    public IndexShard getPrimaryIndexShard() {
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            if (indicesService != null) {
                if (indicesService.hasIndex("test")) {
                    IndexService indexService = indicesService.indexServiceSafe("test");
                    IndexShard indexShard = indexService.shard(0);
                    if (indexShard != null) {
                        if (indexShard.shardRouting.primary()) {
                            return indexShard;
                        }
                    }
                }
            }
        }
        return null;
    }
}
