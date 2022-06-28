/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.ObjectFloatHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.function.Function;

public class Gateway {

    private static final Logger logger = LogManager.getLogger(Gateway.class);

    private final ClusterService clusterService;

    private final TransportNodesListGatewayMetaState listGatewayMetaState;

    private final int minimumMasterNodes;

    public Gateway(
        final Settings settings,
        final ClusterService clusterService,
        final TransportNodesListGatewayMetaState listGatewayMetaState
    ) {
        this.clusterService = clusterService;
        this.listGatewayMetaState = listGatewayMetaState;
        this.minimumMasterNodes = ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
    }

    public void performStateRecovery(final GatewayStateRecoveredListener listener) {
        final String[] nodesIds = clusterService.state().nodes().getMasterNodes().keys().toArray(String.class);
        logger.trace("performing state recovery from {}", Arrays.toString(nodesIds));
        final TransportNodesListGatewayMetaState.NodesGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds, null).actionGet();

        final int requiredAllocation = Math.max(1, minimumMasterNodes);

        if (nodesState.hasFailures()) {
            for (final FailedNodeException failedNodeException : nodesState.failures()) {
                logger.warn("failed to fetch state from node", failedNodeException);
            }
        }

        final ObjectFloatHashMap<Index> indices = new ObjectFloatHashMap<>();
        Metadata electedGlobalState = null;
        int found = 0;
        for (final TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
            if (nodeState.metadata() == null) {
                continue;
            }
            found++;
            if (electedGlobalState == null) {
                electedGlobalState = nodeState.metadata();
            } else if (nodeState.metadata().version() > electedGlobalState.version()) {
                electedGlobalState = nodeState.metadata();
            }
            for (final IndexMetadata indexMetadata : nodeState.metadata().indices().values()) {
                indices.addTo(indexMetadata.getIndex(), 1);
            }
        }
        if (found < requiredAllocation) {
            listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");
            return;
        }
        // update the global state, and clean the indices, we elect them in the next phase
        final Metadata.Builder metadataBuilder = Metadata.builder(electedGlobalState).removeAllIndices();

        assert indices.containsKey(null) == false;
        final Object[] keys = indices.keys;
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                final Index index = (Index) keys[i];
                IndexMetadata electedIndexMetadata = null;
                int indexMetadataCount = 0;
                for (final TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
                    if (nodeState.metadata() == null) {
                        continue;
                    }
                    final IndexMetadata indexMetadata = nodeState.metadata().index(index);
                    if (indexMetadata == null) {
                        continue;
                    }
                    if (electedIndexMetadata == null) {
                        electedIndexMetadata = indexMetadata;
                    } else if (indexMetadata.getVersion() > electedIndexMetadata.getVersion()) {
                        electedIndexMetadata = indexMetadata;
                    }
                    indexMetadataCount++;
                }
                if (electedIndexMetadata != null) {
                    if (indexMetadataCount < requiredAllocation) {
                        logger.debug("[{}] found [{}], required [{}], not adding", index, indexMetadataCount, requiredAllocation);
                    } // TODO if this logging statement is correct then we are missing an else here

                    metadataBuilder.put(electedIndexMetadata, false);
                }
            }
        }
        ClusterState recoveredState = Function.<ClusterState>identity()
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .apply(ClusterState.builder(clusterService.getClusterName()).metadata(metadataBuilder).build());

        listener.onSuccess(recoveredState);
    }

    public interface GatewayStateRecoveredListener {
        void onSuccess(ClusterState build);

        void onFailure(String s);
    }
}
