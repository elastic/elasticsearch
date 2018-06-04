package org.elasticsearch.cluster;/*
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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class IndexingWithMappingUpdateIT extends ESIntegTestCase {
    private String index = "test";
    private String type = "type1";
    private String masterName;
    private String primaryNodeName;
    private String replicaNodeName;
    private Client client;
    private InternalTestCluster cluster;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Before
    public void layout() throws Exception {
        // dedicated master + 1 primary + 1 replica
        cluster = internalCluster();
        masterName = cluster.startMasterOnlyNode();
        List<String> nodeNames = cluster.startDataOnlyNodes(2);

        assertAcked(admin().indices()
            .create(createIndexRequest(index)
                .settings(Settings.builder()
                    .put("index.number_of_replicas", 1)
                    .put("index.number_of_shards", 1)))
            .get());


        client = cluster.client();

        //  force the primary to be fully allocated before starting the replica
        client.prepareIndex(index, type, "d").setSource("{ }", XContentType.JSON).get();

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(index).shard(0).primaryShard().currentNodeId();
        primaryNodeName = clusterState.getRoutingNodes().node(nodeId).node().getName();

        client = cluster.client(primaryNodeName);

        replicaNodeName = nodeNames.stream().filter(name -> !name.equals(primaryNodeName)).findFirst().get();
    }

    /**
     title Replication Sequence, case 1

     Client -> Primary: doc1
     Primary -> Master: Update mapping
     Master -> Primary: mapping update
     Primary -> Master: ack mapping update
     Primary -> Primary: updates local mapping

     Client -> Primary: doc2
     Primary -> Replica: doc2


     Master -> Replica: mapping update
     Replica -> Master: ack mapping update
     Master -> Primary: all nodes acked
     Primary -> Replica: doc1
     */
    public void testRaiseConditionOnSecondDocWithMappingUpdate() throws Exception {
        int docs = randomIntBetween(5, 10);

        final CountDownLatch prestartLatch = new CountDownLatch(1 + docs);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch sendSecondDocLatch = new CountDownLatch(1);

        MockTransportService replicaTransportService =
            ((MockTransportService) cluster.getInstance(TransportService.class, replicaNodeName));

        // tracer on replica to slow down consumption of cluster state with mapping update requests
        // internal:discovery/zen/publish/sent
        // internal:discovery/zen/publish/commit
        replicaTransportService.addTracer(new MockTransportService.Tracer() {
            @Override
            public void receivedRequest(long requestId, String action) {
                boolean clusterStateCommit = action.equals(PublishClusterStateAction.COMMIT_ACTION_NAME);
                boolean clusterStateWrite = action.equals(PublishClusterStateAction.SEND_ACTION_NAME);

                if (clusterStateCommit) {
                    sendSecondDocLatch.countDown();
                }
                if (clusterStateCommit || clusterStateWrite) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                super.receivedRequest(requestId, action);
            }
        });

        final List<IndexResponse> indexResponses = new CopyOnWriteArrayList<>();
        Thread[] threads = new Thread[docs + 1];
        threads[0] = new Thread(() -> {
            try {
                prestartLatch.countDown();
                startLatch.await();

                IndexResponse indexResponse = client.prepareIndex(index, type, "doc")
                    .setSource("{ \"f\": \"normal\"}", XContentType.JSON).get();
                indexResponses.add(indexResponse);
            } catch (InterruptedException e) {
                // ignore
            }
        });

        for(int i = 0; i < docs; i++) {
            IndexRequestBuilder builder = client.prepareIndex(index, type, "doc_" + i)
                .setSource("{ \"f\": \"normal\"}", XContentType.JSON);
            threads[i + 1] = new Thread(() -> {
                try {
                    prestartLatch.countDown();
                    sendSecondDocLatch.await();

                    IndexResponse indexResponse = builder.get();
                    indexResponses.add(indexResponse);
                } catch (InterruptedException e) {
                    // ignore
                }
            });
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].setName("index-" + i);
            threads[i].start();
        }

        prestartLatch.await();
        startLatch.countDown();

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        List<String> failureMessages = indexResponses.stream()
            .map(r -> r.getShardInfo().getFailures())
            .flatMap(Arrays::stream)
            .map(failure -> failure.nodeId() + ": " + failure.reason())
            .collect(Collectors.toList());
        assertThat(failureMessages.toString(), failureMessages, hasSize(0));
    }
}
