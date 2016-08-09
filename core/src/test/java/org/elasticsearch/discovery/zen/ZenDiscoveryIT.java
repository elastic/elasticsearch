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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCustomMetaData;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@ESIntegTestCase.SuppressLocalMode
@TestLogging("_root:DEBUG")
public class ZenDiscoveryIT extends ESIntegTestCase {

    private Version previousMajorVersion;

    @Before
    public void computePrevMajorVersion() {
        Version previousMajor;
        // find a GA build whose major version is <N
        do {
            previousMajor = VersionUtils.randomVersion(random());
        } while (previousMajor.onOrAfter(Version.CURRENT.minimumCompatibilityVersion())
                || previousMajor.isAlpha()
                || previousMajor.isBeta()
                || previousMajor.isRC());
        previousMajorVersion = previousMajor;
    }

    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = Settings.builder()
                .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s")
                .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1")
                .put("discovery.type", "zen")
                .build();

        Settings masterNodeSettings = Settings.builder()
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, masterNodeSettings).get();
        Settings dateNodeSettings = Settings.builder()
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
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
        int numRecoveriesBeforeNewMaster = r.shardRecoveryStates().get("test").size();

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
        int numRecoveriesAfterNewMaster = r.shardRecoveryStates().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

    public void testNodeFailuresAreProcessedOnce() throws ExecutionException, InterruptedException, IOException {
        Settings defaultSettings = Settings.builder()
                .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s")
                .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1")
                .put("discovery.type", "zen")
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

    public void testNodeRejectsClusterStateWithWrongMasterNode() throws Exception {
        Settings settings = Settings.builder()
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
            if (discoveryNode.getName().equals(noneMasterNode)) {
                node = discoveryNode;
            }
        }
        assert node != null;

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes())
                .add(new DiscoveryNode("abc", new LocalTransportAddress("abc"), emptyMap(),
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
        Settings nodeSettings = Settings.builder()
            .put("discovery.type", "zen") // <-- To override the local setting if set externally
            .build();
        String masterOnlyNode = internalCluster().startMasterOnlyNode(nodeSettings);
        String node1 = internalCluster().startNode(nodeSettings);
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
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new CustomMetaData(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
        }
    }

    public void testHandleNodeJoin_incompatibleMinVersion() throws UnknownHostException {
        Settings nodeSettings = Settings.builder()
                .put("discovery.type", "zen") // <-- To override the local setting if set externally
                .build();
        String nodeName = internalCluster().startNode(nodeSettings);
        ZenDiscovery zenDiscovery = (ZenDiscovery) internalCluster().getInstance(Discovery.class, nodeName);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
        DiscoveryNode node = new DiscoveryNode("_node_id", new InetSocketTransportAddress(InetAddress.getByName("0.0.0.0"), 0),
                emptyMap(), emptySet(), previousMajorVersion);
        final AtomicReference<IllegalStateException> holder = new AtomicReference<>();
        zenDiscovery.handleJoinRequest(node, clusterService.state(), new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(Exception e) {
                holder.set((IllegalStateException) e);
            }
        });

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().getMessage(), equalTo("Can't handle join request from a node with a version [" + previousMajorVersion
                + "] that is lower than the minimum compatible version [" + Version.CURRENT.minimumCompatibilityVersion() + "]"));
    }

    public void testJoinElectedMaster_incompatibleMinVersion() {
        ElectMasterService electMasterService = new ElectMasterService(Settings.EMPTY);

        DiscoveryNode node = new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), emptyMap(),
                Collections.singleton(DiscoveryNode.Role.MASTER), Version.CURRENT);
        assertThat(electMasterService.electMaster(Collections.singletonList(node)), sameInstance(node));
        node = new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), emptyMap(), emptySet(), previousMajorVersion);
        assertThat("Can't join master because version " + previousMajorVersion
                + " is lower than the minimum compatable version " + Version.CURRENT + " can support",
                electMasterService.electMaster(Collections.singletonList(node)), nullValue());
    }

    public void testDiscoveryStats() throws IOException {
        String expectedStatsJsonResponse = "{\n" +
                "  \"discovery\" : {\n" +
                "    \"cluster_state_queue\" : {\n" +
                "      \"total\" : 0,\n" +
                "      \"pending\" : 0,\n" +
                "      \"committed\" : 0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Settings nodeSettings = Settings.builder()
                .put("discovery.type", "zen") // <-- To override the local setting if set externally
                .build();
        internalCluster().startNode(nodeSettings);

        logger.info("--> request node discovery stats");
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().setDiscovery(true).get();
        assertThat(statsResponse.getNodes().size(), equalTo(1));

        DiscoveryStats stats = statsResponse.getNodes().get(0).getDiscoveryStats();
        assertThat(stats.getQueueStats(), notNullValue());
        assertThat(stats.getQueueStats().getTotal(), equalTo(0));
        assertThat(stats.getQueueStats().getCommitted(), equalTo(0));
        assertThat(stats.getQueueStats().getPending(), equalTo(0));

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(builder.string(), equalTo(expectedStatsJsonResponse));
    }
}
