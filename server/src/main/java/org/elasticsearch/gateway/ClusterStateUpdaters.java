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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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

    static ClusterState upgradeAndArchiveUnknownOrInvalidSettings(final ClusterState clusterState,
                                                                  final ClusterSettings clusterSettings) {
        final MetaData.Builder metaDataBuilder = MetaData.builder(clusterState.metaData());

        metaDataBuilder.persistentSettings(
                clusterSettings.archiveUnknownOrInvalidSettings(
                        clusterSettings.upgradeSettings(metaDataBuilder.persistentSettings()),
                        e -> logUnknownSetting("persistent", e),
                        (e, ex) -> logInvalidSetting("persistent", e, ex)));
        metaDataBuilder.transientSettings(
                clusterSettings.archiveUnknownOrInvalidSettings(
                        clusterSettings.upgradeSettings(metaDataBuilder.transientSettings()),
                        e -> logUnknownSetting("transient", e),
                        (e, ex) -> logInvalidSetting("transient", e, ex)));
        return ClusterState.builder(clusterState).metaData(metaDataBuilder).build();
    }

    private static void logUnknownSetting(final String settingType, final Map.Entry<String, String> e) {
        logger.warn("ignoring unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private static void logInvalidSetting(final String settingType, final Map.Entry<String, String> e,
                                          final IllegalArgumentException ex) {
        logger.warn(() -> new ParameterizedMessage("ignoring invalid {} setting: [{}] with value [{}]; archiving",
                settingType, e.getKey(), e.getValue()), ex);
    }

    static ClusterState recoverClusterBlocks(final ClusterState state) {
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());

        if (MetaData.SETTING_READ_ONLY_SETTING.get(state.metaData().settings())) {
            blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
        }

        if (MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(state.metaData().settings())) {
            blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        }

        for (final IndexMetaData indexMetaData : state.metaData()) {
            blocks.addBlocks(indexMetaData);
        }

        return ClusterState.builder(state).blocks(blocks).build();
    }

    static ClusterState updateRoutingTable(final ClusterState state) {
        // initialize all index routing tables as empty
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
        for (final ObjectCursor<IndexMetaData> cursor : state.metaData().indices().values()) {
            routingTableBuilder.addAsRecovery(cursor.value);
        }
        // start with 0 based versions for routing table
        routingTableBuilder.version(0);
        return ClusterState.builder(state).routingTable(routingTableBuilder.build()).build();
    }

    static ClusterState removeStateNotRecoveredBlock(final ClusterState state) {
        return ClusterState.builder(state)
                .blocks(ClusterBlocks.builder()
                        .blocks(state.blocks()).removeGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build();
    }

    public static ClusterState addStateNotRecoveredBlock(ClusterState state) {
        return ClusterState.builder(state)
                .blocks(ClusterBlocks.builder()
                        .blocks(state.blocks()).addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build();
    }

    static ClusterState mixCurrentStateAndRecoveredState(final ClusterState currentState, final ClusterState recoveredState) {
        assert currentState.metaData().indices().isEmpty();

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                .blocks(currentState.blocks())
                .blocks(recoveredState.blocks());

        final MetaData.Builder metaDataBuilder = MetaData.builder(recoveredState.metaData());
        // automatically generate a UID for the metadata if we need to
        metaDataBuilder.generateClusterUuidIfNeeded();

        for (final IndexMetaData indexMetaData : recoveredState.metaData()) {
            metaDataBuilder.put(indexMetaData, false);
        }

        return ClusterState.builder(currentState)
                .blocks(blocks)
                .metaData(metaDataBuilder)
                .build();
    }

    public static ClusterState hideStateIfNotRecovered(ClusterState state) {
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());
            blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
            blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            for (IndexMetaData indexMetaData: state.metaData()) {
                blocks.removeIndexBlocks(indexMetaData.getIndex().getName());
            }
            final MetaData metaData = MetaData.builder()
                    .clusterUUID(state.metaData().clusterUUID())
                    .coordinationMetaData(state.metaData().coordinationMetaData())
                    .build();

            return ClusterState.builder(state)
                    .metaData(metaData)
                    .blocks(blocks.build())
                    .build();
        }
        return state;
    }

}
