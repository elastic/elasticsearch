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

package org.elasticsearch.discovery.zen;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@Slow
public class ZenDiscoveryTests extends ElasticsearchIntegrationTest {

    @Test
    public void testChangeRejoinOnMasterOptionIsDynamic() throws Exception {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "zen") // <-- To override the local setting if set externally
                .build();
        String nodeName = internalCluster().startNode(nodeSettings);
        ZenDiscovery zenDiscovery = (ZenDiscovery) internalCluster().getInstance(Discovery.class, nodeName);
        assertThat(zenDiscovery.isRejoinOnMasterGone(), is(true));

        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder().put(ZenDiscovery.SETTING_REJOIN_ON_MASTER_GONE, false))
                .get();

        assertThat(zenDiscovery.isRejoinOnMasterGone(), is(false));
    }

    @Test
    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "zen")
                .build();

        Settings masterNodeSettings = ImmutableSettings.builder()
                .put("node.data", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, masterNodeSettings).get();
        Settings dateNodeSettings = ImmutableSettings.builder()
                .put("node.master", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, dateNodeSettings).get();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        createIndex("test");
        ensureSearchable("test");
        RecoveryResponse r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesBeforeNewMaster = r.shardResponses().get("test").size();

        final String oldMaster = internalCluster().getMasterName();
        internalCluster().stopCurrentMasterNode();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                String current = internalCluster().getMasterName();
                assertThat(current, notNullValue());
                assertThat(current, not(equalTo(oldMaster)));
            }
        });
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewMaster = r.shardResponses().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

    @Test
    @TestLogging(value = "action.admin.cluster.health:TRACE")
    public void testNodeFailuresAreProcessedOnce() throws ExecutionException, InterruptedException, IOException {
        Settings defaultSettings = ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "zen")
                .build();

        Settings masterNodeSettings = ImmutableSettings.builder()
                .put("node.data", false)
                .put(defaultSettings)
                .build();
        String master = internalCluster().startNode(masterNodeSettings);
        Settings dateNodeSettings = ImmutableSettings.builder()
                .put("node.master", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, dateNodeSettings).get();
        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        final ArrayList<ClusterState> statesFound = new ArrayList<>();
        final CountDownLatch nodesStopped = new CountDownLatch(1);
        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                statesFound.add(event.state());
                try {
                    // block until both nodes have stopped to accumulate node failures
                    nodesStopped.await();
                } catch (InterruptedException e) {
                    //meh
                }
            }
        });

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();
        nodesStopped.countDown();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get(); // wait for all to be processed
        assertThat(statesFound, Matchers.hasSize(2));
    }

    @Test
    public void testNodeRejectsClusterStateWithWrongMasterNode() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("discovery.type", "zen")
                .build();
        List<String> nodeNames = internalCluster().startNodesAsync(2, settings).get();
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();

        List<String> nonMasterNodes = new ArrayList<>(nodeNames);
        nonMasterNodes.remove(internalCluster().getMasterName());
        String noneMasterNode = nonMasterNodes.get(0);

        ClusterState state = internalCluster().getInstance(ClusterService.class).state();
        DiscoveryNode node = null;
        for (DiscoveryNode discoveryNode : state.nodes()) {
            if (discoveryNode.name().equals(noneMasterNode)) {
                node = discoveryNode;
            }
        }
        assert node != null;

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes())
                .put(new DiscoveryNode("abc", new LocalTransportAddress("abc"), Version.CURRENT)).masterNodeId("abc");
        ClusterState.Builder builder = ClusterState.builder(state);
        builder.nodes(nodes);
        BytesStreamOutput bStream = new BytesStreamOutput();
        StreamOutput stream = CompressorFactory.defaultCompressor().streamOutput(bStream);
        stream.setVersion(node.version());
        ClusterState.Builder.writeTo(builder.build(), stream);
        stream.close();
        BytesReference bytes = bStream.bytes();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        internalCluster().getInstance(TransportService.class, noneMasterNode).sendRequest(node, PublishClusterStateAction.ACTION_NAME, new BytesTransportRequest(bytes, Version.CURRENT), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

            @Override
            public void handleResponse(TransportResponse.Empty response) {
                super.handleResponse(response);
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                super.handleException(exp);
                reference.set(exp);
                latch.countDown();
            }
        });
        latch.await();
        assertThat(reference.get(), notNullValue());
        assertThat(ExceptionsHelper.detailedMessage(reference.get()), containsString("cluster state from a different master then the current one, rejecting "));
    }
}
