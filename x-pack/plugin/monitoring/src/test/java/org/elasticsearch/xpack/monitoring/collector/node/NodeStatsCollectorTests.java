/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeStatsCollectorTests extends BaseCollectorTestCase {

    public void testShouldCollectReturnsTrue() {
        final boolean isElectedMaster = true;

        final NodeStatsCollector collector = new NodeStatsCollector(clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));
    }

    public void testDoCollectWithFailures() throws Exception {
        final TimeValue timeout = randomPositiveTimeValue();
        withCollectionTimeout(NodeStatsCollector.NODE_STATS_TIMEOUT, timeout);

        final NodesStatsResponse nodesStatsResponse = mock(NodesStatsResponse.class);
        when(nodesStatsResponse.hasFailures()).thenReturn(true);

        final FailedNodeException exception = new FailedNodeException("_node_id", "_msg", new Exception());
        when(nodesStatsResponse.failures()).thenReturn(Collections.singletonList(exception));

        final Client client = mock(Client.class);
        thenReturnNodeStats(client, timeout, nodesStatsResponse);

        final NodeStatsCollector collector = new NodeStatsCollector(clusterService, licenseState, client);
        assertEquals(timeout, collector.getCollectionTimeout());

        final FailedNodeException e = expectThrows(
            FailedNodeException.class,
            () -> collector.doCollect(randomMonitoringNode(random()), randomNonNegativeLong(), clusterState)
        );
        assertEquals(exception, e);

        assertWarnings(
            "[xpack.monitoring.collection.node.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release. See the deprecation documentation for the next major version."
        );
    }

    public void testDoCollect() throws Exception {
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(NodeStatsCollector.NODE_STATS_TIMEOUT, timeout);

        final boolean isMaster = randomBoolean();
        whenLocalNodeElectedMaster(isMaster);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final MonitoringDoc.Node node = randomMonitoringNode(random());

        final NodesStatsResponse nodesStatsResponse = mock(NodesStatsResponse.class);
        when(nodesStatsResponse.hasFailures()).thenReturn(false);

        final NodeStats nodeStats = mock(NodeStats.class);
        when(nodesStatsResponse.getNodes()).thenReturn(Collections.singletonList(nodeStats));

        final long timestamp = randomNonNegativeLong();
        when(nodeStats.getTimestamp()).thenReturn(timestamp);

        final Client client = mock(Client.class);
        thenReturnNodeStats(client, timeout, nodesStatsResponse);

        final NodeStatsCollector collector = new NodeStatsCollector(clusterService, licenseState, client);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> results = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metadata();
        verify(metadata).clusterUUID();

        assertEquals(1, results.size());

        final MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(NodeStatsMonitoringDoc.class));

        final NodeStatsMonitoringDoc document = (NodeStatsMonitoringDoc) monitoringDoc;
        assertThat(document.getCluster(), equalTo(clusterUUID));
        assertThat(document.getTimestamp(), equalTo(timestamp));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getNode(), equalTo(node));
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), equalTo(NodeStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.isNodeMaster(), equalTo(isMaster));
        assertThat(document.getNodeId(), equalTo(node.getUUID()));
        assertThat(document.getNodeStats(), is(nodeStats));
        assertThat(document.isMlockall(), equalTo(BootstrapInfo.isMemoryLocked()));

        assertWarnings(
            "[xpack.monitoring.collection.node.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release. See the deprecation documentation for the next major version."
        );
    }

    public void testDoCollectThrowsTimeout() throws Exception {
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 120));
        withCollectionTimeout(NodeStatsCollector.NODE_STATS_TIMEOUT, timeout);

        final boolean isMaster = randomBoolean();
        whenLocalNodeElectedMaster(isMaster);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final MonitoringDoc.Node node = randomMonitoringNode(random());

        final NodesStatsResponse nodesStatsResponse = mock(NodesStatsResponse.class);
        when(nodesStatsResponse.hasFailures()).thenReturn(true);
        when(nodesStatsResponse.failures()).thenReturn(
            List.of(new FailedNodeException("node", "msg", new ElasticsearchTimeoutException("test")))
        );

        final Client client = mock(Client.class);
        thenReturnNodeStats(client, timeout, nodesStatsResponse);

        final NodeStatsCollector collector = new NodeStatsCollector(clusterService, licenseState, client);
        assertEquals(timeout, collector.getCollectionTimeout());

        final long interval = randomNonNegativeLong();

        expectThrows(ElasticsearchTimeoutException.class, () -> collector.doCollect(node, interval, clusterState));

        assertWarnings(
            "[xpack.monitoring.collection.node.stats.timeout] setting was deprecated in Elasticsearch and will be removed "
                + "in a future release. See the deprecation documentation for the next major version."
        );
    }

    private void thenReturnNodeStats(final Client client, final TimeValue timeout, final NodesStatsResponse nodesStatsResponse) {
        @SuppressWarnings("unchecked")
        final ActionFuture<NodesStatsResponse> future = (ActionFuture<NodesStatsResponse>) mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(nodesStatsResponse);

        final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(clusterAdminClient.nodesStats(any(NodesStatsRequest.class))).thenReturn(future);

        final AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        when(client.admin()).thenReturn(adminClient);
    }
}
