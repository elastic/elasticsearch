/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.ClusterSettings;

import java.util.Map;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class ClusterStateUpdaters {
    private static final Logger logger = LogManager.getLogger(ClusterStateUpdaters.class);

    static ClusterState setLocalNode(final ClusterState clusterState, DiscoveryNode localNode) {
        return ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .build();
    }

    static ClusterState upgradeAndArchiveUnknownOrInvalidSettings(final ClusterState clusterState, final ClusterSettings clusterSettings) {
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());

        metadataBuilder.persistentSettings(
            clusterSettings.archiveUnknownOrInvalidSettings(
                clusterSettings.upgradeSettings(metadataBuilder.persistentSettings()),
                e -> logUnknownSetting("persistent", e),
                (e, ex) -> logInvalidSetting("persistent", e, ex)
            )
        );
        metadataBuilder.transientSettings(
            clusterSettings.archiveUnknownOrInvalidSettings(
                clusterSettings.upgradeSettings(metadataBuilder.transientSettings()),
                e -> logUnknownSetting("transient", e),
                (e, ex) -> logInvalidSetting("transient", e, ex)
            )
        );
        return ClusterState.builder(clusterState).metadata(metadataBuilder).build();
    }

    private static void logUnknownSetting(final String settingType, final Map.Entry<String, String> e) {
        logger.warn("ignoring unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private static void logInvalidSetting(final String settingType, final Map.Entry<String, String> e, final IllegalArgumentException ex) {
        logger.warn(
            () -> new ParameterizedMessage(
                "ignoring invalid {} setting: [{}] with value [{}]; archiving",
                settingType,
                e.getKey(),
                e.getValue()
            ),
            ex
        );
    }

    static ClusterState recoverClusterBlocks(final ClusterState state) {
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());

        if (Metadata.SETTING_READ_ONLY_SETTING.get(state.metadata().settings())) {
            blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
        }

        if (Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(state.metadata().settings())) {
            blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        }

        for (final IndexMetadata indexMetadata : state.metadata()) {
            blocks.addBlocks(indexMetadata);
        }

        return ClusterState.builder(state).blocks(blocks).build();
    }

    static ClusterState initializeRoutingTable(final ClusterState state) {
        assert state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) : state;

        // initialize all index routing tables as needing recovery
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
        for (final IndexMetadata indexMetadata : state.metadata().indices().values()) {
            routingTableBuilder.addAsRecovery(indexMetadata);
        }

        return ClusterState.builder(state)
            // start with 0 based versions for routing table
            .routingTable(routingTableBuilder.version(0).build())
            // remove STATE_NOT_RECOVERED_BLOCK
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
    }

    public static ClusterState addStateNotRecoveredBlock(ClusterState state) {
        assert state.routingTable().version() == 0 : state;
        assert state.routingTable().iterator().hasNext() == false : state;
        return ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
    }

    public static ClusterState hideStateIfNotRecovered(ClusterState state) {
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());
            blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
            blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            for (IndexMetadata indexMetadata : state.metadata()) {
                blocks.removeIndexBlocks(indexMetadata.getIndex().getName());
            }
            final Metadata metadata = Metadata.builder()
                .clusterUUID(state.metadata().clusterUUID())
                .coordinationMetadata(state.metadata().coordinationMetadata())
                .build();

            return ClusterState.builder(state).metadata(metadata).blocks(blocks.build()).build();
        }
        return state;
    }

}
