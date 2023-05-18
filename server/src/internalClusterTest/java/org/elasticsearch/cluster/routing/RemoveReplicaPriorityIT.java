/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.hamcrest.Matchers.hasSize;

public class RemoveReplicaPriorityIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "testindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testReplicaRemovalPriority() throws Exception {

        internalCluster().ensureAtLeastNumDataNodes(3);

        final AtomicBoolean blockRecoveriesRef = new AtomicBoolean();

        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            final MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (blockRecoveriesRef.get() && action.equals(START_RECOVERY)) {
                    return; // drop start-recovery requests to block the recovery
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final String dataNodeIdFilter = clusterAdmin().prepareState()
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getId)
            .limit(3)
            .collect(Collectors.joining(","));
        final String excludedDataNodeId = dataNodeIdFilter.substring(0, dataNodeIdFilter.indexOf(','));

        createIndex(
            INDEX_NAME,
            indexSettings(1, 3).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._id", dataNodeIdFilter)
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._id", excludedDataNodeId)
                .build()
        );

        assertBusy(() -> {
            final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState()
                .clear()
                .setRoutingTable(true)
                .get()
                .getState()
                .routingTable()
                .index("testindex")
                .shard(0);
            assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(STARTED), hasSize(2));
            assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(UNASSIGNED), hasSize(2));
        });

        blockRecoveriesRef.set(true);
        updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._id"), INDEX_NAME);

        assertBusy(() -> {
            final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState()
                .clear()
                .setRoutingTable(true)
                .get()
                .getState()
                .routingTable()
                .index("testindex")
                .shard(0);
            assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(STARTED), hasSize(2));
            assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(INITIALIZING), hasSize(1));
            assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(UNASSIGNED), hasSize(1));
        });

        if (randomBoolean()) {
            setReplicaCount(2, INDEX_NAME);

            assertBusy(() -> {
                final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState()
                    .clear()
                    .setRoutingTable(true)
                    .get()
                    .getState()
                    .routingTable()
                    .index("testindex")
                    .shard(0);
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(STARTED), hasSize(2));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(INITIALIZING), hasSize(1));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(UNASSIGNED), hasSize(0));
            });
        }

        if (randomBoolean()) {
            setReplicaCount(1, INDEX_NAME);

            assertBusy(() -> {
                final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState()
                    .clear()
                    .setRoutingTable(true)
                    .get()
                    .getState()
                    .routingTable()
                    .index("testindex")
                    .shard(0);
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(STARTED), hasSize(2));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(INITIALIZING), hasSize(0));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(UNASSIGNED), hasSize(0));
            });
        }

        if (randomBoolean()) {
            setReplicaCount(0, INDEX_NAME);

            assertBusy(() -> {
                final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState()
                    .clear()
                    .setRoutingTable(true)
                    .get()
                    .getState()
                    .routingTable()
                    .index("testindex")
                    .shard(0);
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(STARTED), hasSize(1));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(INITIALIZING), hasSize(0));
                assertThat(indexShardRoutingTable.toString(), indexShardRoutingTable.shardsWithState(UNASSIGNED), hasSize(0));
            });
        }

    }

}
