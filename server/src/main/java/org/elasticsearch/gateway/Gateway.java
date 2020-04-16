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

import com.carrotsearch.hppc.ObjectFloatHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.function.Function;

public class Gateway {

    private static final Logger logger = LogManager.getLogger(Gateway.class);

    private final ClusterService clusterService;

    private final NodeClient client;

    public Gateway(final ClusterService clusterService, final NodeClient client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        final String[] nodesIds = clusterService.state().nodes().getMasterNodes().keys().toArray(String.class);
        logger.trace("performing state recovery from {}", Arrays.toString(nodesIds));
        var request = new TransportNodesListGatewayMetaState.Request(nodesIds);
        PlainActionFuture<TransportNodesListGatewayMetaState.NodesGatewayMetaState> future = PlainActionFuture.newFuture();
        client.executeLocally(TransportNodesListGatewayMetaState.TYPE, request, future);
        final TransportNodesListGatewayMetaState.NodesGatewayMetaState nodesState = future.actionGet();

        final int requiredAllocation = 1;

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
            for (final ObjectCursor<IndexMetadata> cursor : nodeState.metadata().indices().values()) {
                indices.addTo(cursor.value.getIndex(), 1);
            }
        }
        if (found < requiredAllocation) {
            listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");
            return;
        }
        // update the global state, and clean the indices, we elect them in the next phase
        final Metadata.Builder metadataBuilder = Metadata.builder(electedGlobalState).removeAllIndices();

        assert !indices.containsKey(null);
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
