/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterHistoryTests extends ESTestCase {

    private ClusterState nullMasterClusterState;
    private ClusterState node1MasterClusterState;
    private ClusterState node2MasterClusterState;
    private ClusterState node3MasterClusterState;
    private static final String TEST_SOURCE = "test";

    @Before
    public void setup() throws Exception {
        String node1 = randomNodeId();
        String node2 = randomNodeId();
        String node3 = randomNodeId();
        nullMasterClusterState = createClusterState(null);
        node1MasterClusterState = createClusterState(node1);
        node2MasterClusterState = createClusterState(node2);
        node3MasterClusterState = createClusterState(node3);
    }

    public void testGetBasicUse() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        assertNull(masterHistory.getMostRecentMaster());
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertNull(masterHistory.getMostRecentMaster());
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node1MasterClusterState.nodes().getMasterNode()));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node2MasterClusterState.nodes().getMasterNode()));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, node2MasterClusterState));
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node3MasterClusterState.nodes().getMasterNode()));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node3MasterClusterState));
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node1MasterClusterState.nodes().getMasterNode()));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertNull(masterHistory.getMostRecentMaster());
        assertThat(masterHistory.getMostRecentNonNullMaster(), equalTo(node1MasterClusterState.nodes().getMasterNode()));
    }

    public void testHasMasterGoneNull() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        long oneHourAgo = System.currentTimeMillis() - (60 * 60 * 1000);
        when(threadPool.relativeTimeInMillis()).thenReturn(oneHourAgo);
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertTrue(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        assertFalse(masterHistory.hasMasterGoneNullAtLeastNTimes(3));
    }

    public void testTime() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        long oneHourAgo = System.currentTimeMillis() - (60 * 60 * 1000);
        when(threadPool.relativeTimeInMillis()).thenReturn(oneHourAgo);
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, node2MasterClusterState));
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node3MasterClusterState.nodes().getMasterNode()));
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        assertThat(masterHistory.getMostRecentMaster(), equalTo(node3MasterClusterState.nodes().getMasterNode()));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node3MasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
    }

    public void testHasSeenMasterInLastNSeconds() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);

        /*
         * 60 minutes ago we get these master changes:
         * null -> node1 -> node2 -> node3
         * Except for when only null had been master, there has been a non-null master node in the last 5 seconds all along
         */
        long sixtyMinutesAgo = System.currentTimeMillis() - new TimeValue(60, TimeUnit.MINUTES).getMillis();
        when(threadPool.relativeTimeInMillis()).thenReturn(sixtyMinutesAgo);
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, node2MasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));

        /*
         * 40 minutes ago we get these master changes (the master was node3 when this section began):
         * null -> node1 -> null -> null -> node1
         * There has been a non-null master for the last 5 seconds every step at this time
         */
        long fourtyMinutesAgo = System.currentTimeMillis() - new TimeValue(40, TimeUnit.MINUTES).getMillis();
        when(threadPool.relativeTimeInMillis()).thenReturn(fourtyMinutesAgo);
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node3MasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));

        /*
         * 6 seconds ago we get these master changes (it had been set to node1 previously):
         * null -> null
         * Even though the last non-null master was more
         * than 5 seconds ago (and more than the age of history we keep, 30 minutes), the transition from it to null was just now, so we
         * still say that there has been a master recently.
         */
        long sixSecondsAgo = System.currentTimeMillis() - new TimeValue(6, TimeUnit.SECONDS).getMillis();
        when(threadPool.relativeTimeInMillis()).thenReturn(sixSecondsAgo);
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));

        /*
         * Right now we get these master changes (the master was null when this section began):
         * null -> node1
         * Even before the first transition to null, we have no longer seen a non-null master within the last 5 seconds (because we last
         * transitioned from a non-null master 6 seconds ago). After the transition to node1, we again have seen a non-null master in the
         *  last 5 seconds.
         */
        long now = System.currentTimeMillis();
        when(threadPool.relativeTimeInMillis()).thenReturn(now);
        assertFalse(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        assertFalse(masterHistory.hasSeenMasterInLastNSeconds(5));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertTrue(masterHistory.hasSeenMasterInLastNSeconds(5));
    }

    public void testGetNumberOfMasterChanges() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(0));
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(0)); // The first master
                                                                                                          // doesn't count as a
                                                                                                          // change
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(0)); // Nulls don't count
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(0)); // Still no change in the
                                                                                                          // last non-null master
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(0)); // Nulls don't count
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(1)); // Finally a new non-null
                                                                                                          // master
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(1)); // Nulls don't count
        masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(MasterHistory.getNumberOfMasterIdentityChanges(masterHistory.getNodes()), equalTo(2)); // Back to node1, but it's
                                                                                                          // a change from node2
    }

    public void testMaxSize() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        MasterHistory masterHistory = new MasterHistory(threadPool, clusterService);
        for (int i = 0; i < MasterHistory.MAX_HISTORY_SIZE; i++) {
            masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
            masterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        }
        assertThat(masterHistory.getNodes().size(), lessThanOrEqualTo(MasterHistory.MAX_HISTORY_SIZE));

    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private static ClusterState createClusterState(String masterNodeId) throws UnknownHostException {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNodeId != null) {
            DiscoveryNode node = DiscoveryNodeUtils.create(masterNodeId);
            nodesBuilder.masterNodeId(masterNodeId);
            nodesBuilder.add(node);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();
    }
}
