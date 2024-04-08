/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class MinimumMasterNodesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    public void testTwoNodesNoMasterBlock() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(1);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        logger.info("--> start first node");
        String node1Name = internalCluster().startNode(settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().getSize(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> start second node, cluster should be formed");
        String node2Name = internalCluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));
        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));

        state = clusterAdmin().prepareState().get().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(false));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(
            clusterAdmin().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).get().getActiveShards(),
            equalTo(numShards.totalNumShards)
        );
        // flush for simpler debugging
        flushAndRefresh();

        logger.info("--> verify we get the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
        }

        String masterNode = internalCluster().getMasterName();
        String otherNode = node1Name.equals(masterNode) ? node2Name : node1Name;
        logger.info("--> add voting config exclusion for non-master node, to be sure it's not elected");
        client().execute(TransportAddVotingConfigExclusionsAction.TYPE, new AddVotingConfigExclusionsRequest(otherNode)).get();
        logger.info("--> stop master node, no master block should appear");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(masterNode);
        internalCluster().stopNode(masterNode);

        assertBusy(() -> {
            ClusterState clusterState = clusterAdmin().prepareState().setLocal(true).get().getState();
            assertTrue(clusterState.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(true));
        // verify that both nodes are still in the cluster state but there is no master
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.nodes().getMasterNode(), equalTo(null));

        logger.info("--> starting the previous master node again...");
        node2Name = internalCluster().startNode(Settings.builder().put(settings).put(masterDataPathSettings).build());

        clusterHealthResponse = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));
        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));

        state = clusterAdmin().prepareState().get().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(true));

        ensureGreen();

        logger.info("--> verify we get the data back after cluster reform");
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
        }

        logger.info("--> clearing voting config exclusions");
        ClearVotingConfigExclusionsRequest clearRequest = new ClearVotingConfigExclusionsRequest();
        clearRequest.setWaitForRemoval(false);
        client().execute(TransportClearVotingConfigExclusionsAction.TYPE, clearRequest).get();

        masterNode = internalCluster().getMasterName();
        otherNode = node1Name.equals(masterNode) ? node2Name : node1Name;
        logger.info("--> add voting config exclusion for master node, to be sure it's not elected");
        client().execute(TransportAddVotingConfigExclusionsAction.TYPE, new AddVotingConfigExclusionsRequest(masterNode)).get();
        logger.info("--> stop non-master node, no master block should appear");
        Settings otherNodeDataPathSettings = internalCluster().dataPathSettings(otherNode);
        internalCluster().stopNode(otherNode);

        assertBusy(() -> {
            ClusterState state1 = clusterAdmin().prepareState().setLocal(true).get().getState();
            assertThat(state1.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(true));
        });

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(Settings.builder().put(settings).put(otherNodeDataPathSettings).build());

        ensureGreen();
        clusterHealthResponse = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .setWaitForGreenStatus()
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));
        state = clusterAdmin().prepareState().setLocal(true).get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(false));

        state = clusterAdmin().prepareState().get().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ensureGreen();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
        }
    }

    public void testThreeNodesNoMasterBlock() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        logger.info("--> start first 2 nodes");
        internalCluster().startNodes(2, settings);

        ClusterState state;

        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterState state1 = client.admin().cluster().prepareState().setLocal(true).get().getState();
                assertThat(state1.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(true));
            }
        });

        logger.info("--> start one more node");
        internalCluster().startNode(settings);

        ensureGreen();
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = clusterAdmin().prepareState().get().getState();
        assertThat(state.nodes().getSize(), equalTo(3));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        ensureGreen();
        // make sure that all shards recovered before trying to flush
        assertThat(
            clusterAdmin().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).get().isTimedOut(),
            equalTo(false)
        );
        // flush for simpler debugging
        indicesAdmin().prepareFlush().get();

        refresh();
        logger.info("--> verify we get the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
        }

        List<String> nonMasterNodes = new ArrayList<>(
            Sets.difference(Sets.newHashSet(internalCluster().getNodeNames()), Collections.singleton(internalCluster().getMasterName()))
        );
        Settings nonMasterDataPathSettings1 = internalCluster().dataPathSettings(nonMasterNodes.get(0));
        Settings nonMasterDataPathSettings2 = internalCluster().dataPathSettings(nonMasterNodes.get(1));
        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();

        logger.info("--> verify that there is no master anymore on remaining node");
        // spin here to wait till the state is set
        assertBusy(() -> {
            ClusterState st = clusterAdmin().prepareState().setLocal(true).get().getState();
            assertThat(st.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID), equalTo(true));
        });

        logger.info("--> start back the 2 nodes ");
        internalCluster().startNodes(nonMasterDataPathSettings1, nonMasterDataPathSettings2);

        internalCluster().validateClusterFormed();
        ensureGreen();

        state = clusterAdmin().prepareState().get().getState();
        assertThat(state.nodes().getSize(), equalTo(3));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
        }
    }

    public void testCannotCommitStateThreeNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        internalCluster().startNodes(3, settings);
        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        Set<String> otherNodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        otherNodes.remove(master);
        NetworkDisruption partition = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        logger.debug("--> submitting for cluster state to be rejected");
        final ClusterService masterClusterService = internalCluster().clusterService(master);
        masterClusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                logger.debug("--> starting the disruption, preventing cluster state publishing");
                partition.startDisrupting();
                Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .persistentSettings(
                        Settings.builder().put(currentState.metadata().persistentSettings()).put("_SHOULD_NOT_BE_THERE_", true).build()
                    );
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });

        logger.debug("--> waiting for cluster state to be processed/rejected");
        latch.await();

        assertThat(failure.get(), instanceOf(FailedToCommitClusterStateException.class));

        logger.debug("--> check that there is no master in minor partition");
        assertBusy(() -> assertThat(masterClusterService.state().nodes().getMasterNode(), nullValue()));

        // let major partition to elect new master, to ensure that old master is not elected once partition is restored,
        // otherwise persistent setting (which is a part of accepted state on old master) will be propagated to other nodes
        logger.debug("--> wait for master to be elected in major partition");
        assertBusy(() -> {
            DiscoveryNode masterNode = internalCluster().client(randomFrom(otherNodes))
                .admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .nodes()
                .getMasterNode();
            assertThat(masterNode, notNullValue());
            assertThat(masterNode.getName(), not(equalTo(master)));
        });

        partition.stopDisrupting();

        logger.debug("--> waiting for cluster to heal");
        assertNoTimeout(clusterAdmin().prepareHealth().setWaitForNodes("3").setWaitForEvents(Priority.LANGUID));

        for (String node : internalCluster().getNodeNames()) {
            Settings nodeSetting = internalCluster().clusterService(node).state().metadata().settings();
            assertThat(
                node + " processed the cluster state despite of a min master node violation",
                nodeSetting.get("_SHOULD_NOT_BE_THERE_"),
                nullValue()
            );
        }

    }
}
