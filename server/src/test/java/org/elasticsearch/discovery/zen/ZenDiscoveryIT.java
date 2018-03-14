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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCustomMetaData;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@TestLogging("_root:DEBUG")
public class ZenDiscoveryIT extends ESIntegTestCase {

    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = Settings.builder()
                .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s")
                .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1")
                .build();

        Settings masterNodeSettings = Settings.builder()
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodes(2, masterNodeSettings);
        Settings dateNodeSettings = Settings.builder()
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodes(2, dateNodeSettings);
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("4")
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        createIndex("test");
        ensureSearchable("test");
        RecoveryResponse r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesBeforeNewMaster = r.shardRecoveryStates().get("test").size();

        final String oldMaster = internalCluster().getMasterName();
        internalCluster().stopCurrentMasterNode();
        assertBusy(() -> {
            String current = internalCluster().getMasterName();
            assertThat(current, notNullValue());
            assertThat(current, not(equalTo(oldMaster)));
        });
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewMaster = r.shardRecoveryStates().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

    public void testNodeFailuresAreProcessedOnce() throws ExecutionException, InterruptedException, IOException {
        Settings defaultSettings = Settings.builder()
                .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s")
                .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1")
                .build();

        Settings masterNodeSettings = Settings.builder()
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .put(defaultSettings)
                .build();
        String master = internalCluster().startNode(masterNodeSettings);
        Settings dateNodeSettings = Settings.builder()
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodes(2, dateNodeSettings);
        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        final AtomicInteger numUpdates = new AtomicInteger();
        final CountDownLatch nodesStopped = new CountDownLatch(1);
        clusterService.addStateApplier(event -> {
            numUpdates.incrementAndGet();
            try {
                // block until both nodes have stopped to accumulate node failures
                nodesStopped.await();
            } catch (InterruptedException e) {
                //meh
            }
        });

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();
        nodesStopped.countDown();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get(); // wait for all to be processed
        assertThat(numUpdates.get(), either(equalTo(1)).or(equalTo(2))); // due to batching, both nodes can be handled in same CS update
    }

    public void testNodeRejectsClusterStateWithWrongMasterNode() throws Exception {
        List<String> nodeNames = internalCluster().startNodes(2);

        List<String> nonMasterNodes = new ArrayList<>(nodeNames);
        nonMasterNodes.remove(internalCluster().getMasterName());
        String noneMasterNode = nonMasterNodes.get(0);

        ClusterState state = internalCluster().getInstance(ClusterService.class).state();
        DiscoveryNode node = null;
        for (DiscoveryNode discoveryNode : state.nodes()) {
            if (discoveryNode.getName().equals(noneMasterNode)) {
                node = discoveryNode;
            }
        }
        assert node != null;

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes())
                .add(new DiscoveryNode("abc", buildNewFakeTransportAddress(), emptyMap(),
                        emptySet(), Version.CURRENT)).masterNodeId("abc");
        ClusterState.Builder builder = ClusterState.builder(state);
        builder.nodes(nodes);
        BytesReference bytes = PublishClusterStateAction.serializeFullClusterState(builder.build(), node.getVersion());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        internalCluster().getInstance(TransportService.class, noneMasterNode).sendRequest(node, PublishClusterStateAction.SEND_ACTION_NAME,
                new BytesTransportRequest(bytes, Version.CURRENT), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

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
        assertThat(ExceptionsHelper.detailedMessage(reference.get()),
                containsString("cluster state from a different master than the current one, rejecting"));
    }

    public void testHandleNodeJoin_incompatibleClusterState() throws UnknownHostException {
        String masterOnlyNode = internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startNode();
        ZenDiscovery zenDiscovery = (ZenDiscovery) internalCluster().getInstance(Discovery.class, masterOnlyNode);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node1);
        final ClusterState state = clusterService.state();
        MetaData.Builder mdBuilder = MetaData.builder(state.metaData());
        mdBuilder.putCustom(CustomMetaData.TYPE, new CustomMetaData("data"));
        ClusterState stateWithCustomMetaData = ClusterState.builder(state).metaData(mdBuilder).build();

        final AtomicReference<IllegalStateException> holder = new AtomicReference<>();
        DiscoveryNode node = state.nodes().getLocalNode();
        zenDiscovery.handleJoinRequest(node, stateWithCustomMetaData, new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(Exception e) {
                holder.set((IllegalStateException) e);
            }
        });

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().getMessage(), equalTo("failure when sending a validation request to node"));
    }

    public static class CustomMetaData extends TestCustomMetaData {
        public static final String TYPE = "custom_md";

        CustomMetaData(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
        }
    }

    public void testDiscoveryStats() throws Exception {
        String expectedStatsJsonResponse = "{\n" +
                "  \"discovery\" : {\n" +
                "    \"cluster_state_queue\" : {\n" +
                "      \"total\" : 0,\n" +
                "      \"pending\" : 0,\n" +
                "      \"committed\" : 0\n" +
                "    },\n" +
                "    \"published_cluster_states\" : {\n" +
                "      \"full_states\" : 0,\n" +
                "      \"incompatible_diffs\" : 0,\n" +
                "      \"compatible_diffs\" : 0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        internalCluster().startNode();
        ensureGreen(); // ensures that all events are processed (in particular state recovery fully completed)
        assertBusy(() ->
            assertThat(internalCluster().clusterService(internalCluster().getMasterName()).getMasterService().numberOfPendingTasks(),
                equalTo(0))); // see https://github.com/elastic/elasticsearch/issues/24388

        logger.info("--> request node discovery stats");
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().setDiscovery(true).get();
        assertThat(statsResponse.getNodes().size(), equalTo(1));

        DiscoveryStats stats = statsResponse.getNodes().get(0).getDiscoveryStats();
        assertThat(stats.getQueueStats(), notNullValue());
        assertThat(stats.getQueueStats().getTotal(), equalTo(0));
        assertThat(stats.getQueueStats().getCommitted(), equalTo(0));
        assertThat(stats.getQueueStats().getPending(), equalTo(0));

        assertThat(stats.getPublishStats(), notNullValue());
        assertThat(stats.getPublishStats().getFullClusterStateReceivedCount(), equalTo(0L));
        assertThat(stats.getPublishStats().getIncompatibleClusterStateDiffReceivedCount(), equalTo(0L));
        assertThat(stats.getPublishStats().getCompatibleClusterStateDiffReceivedCount(), equalTo(0L));

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(builder.string(), equalTo(expectedStatsJsonResponse));
    }
}
