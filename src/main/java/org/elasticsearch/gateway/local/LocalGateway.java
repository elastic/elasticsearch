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

package org.elasticsearch.gateway.local;

import com.carrotsearch.hppc.ObjectFloatOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.gateway.local.state.meta.LocalGatewayMetaState;
import org.elasticsearch.gateway.local.state.meta.TransportNodesListGatewayMetaState;
import org.elasticsearch.gateway.local.state.shards.LocalGatewayShardsState;
import org.elasticsearch.index.gateway.local.LocalIndexGatewayModule;

/**
 *
 */
public class LocalGateway extends AbstractLifecycleComponent<Gateway> implements Gateway, ClusterStateListener {

    private final ClusterService clusterService;

    private final NodeEnvironment nodeEnv;

    private final LocalGatewayShardsState shardsState;
    private final LocalGatewayMetaState metaState;

    private final TransportNodesListGatewayMetaState listGatewayMetaState;

    private final String initialMeta;
    private final ClusterName clusterName;

    @Inject
    public LocalGateway(Settings settings, ClusterService clusterService, NodeEnvironment nodeEnv,
                        LocalGatewayShardsState shardsState, LocalGatewayMetaState metaState,
                        TransportNodesListGatewayMetaState listGatewayMetaState, ClusterName clusterName) {
        super(settings);
        this.clusterService = clusterService;
        this.nodeEnv = nodeEnv;
        this.metaState = metaState;
        this.listGatewayMetaState = listGatewayMetaState;
        this.clusterName = clusterName;

        this.shardsState = shardsState;

        clusterService.addLast(this);

        // we define what is our minimum "master" nodes, use that to allow for recovery
        this.initialMeta = componentSettings.get("initial_meta", settings.get("discovery.zen.minimum_master_nodes", "1"));
    }

    @Override
    public String type() {
        return "local";
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        clusterService.remove(this);
    }

    @Override
    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        ObjectOpenHashSet<String> nodesIds = ObjectOpenHashSet.from(clusterService.state().nodes().masterNodes().keys());
        logger.trace("performing state recovery from {}", nodesIds);
        TransportNodesListGatewayMetaState.NodesLocalGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds.toArray(String.class), null).actionGet();


        int requiredAllocation = 1;
        try {
            if ("quorum".equals(initialMeta)) {
                if (nodesIds.size() > 2) {
                    requiredAllocation = (nodesIds.size() / 2) + 1;
                }
            } else if ("quorum-1".equals(initialMeta) || "half".equals(initialMeta)) {
                if (nodesIds.size() > 2) {
                    requiredAllocation = ((1 + nodesIds.size()) / 2);
                }
            } else if ("one".equals(initialMeta)) {
                requiredAllocation = 1;
            } else if ("full".equals(initialMeta) || "all".equals(initialMeta)) {
                requiredAllocation = nodesIds.size();
            } else if ("full-1".equals(initialMeta) || "all-1".equals(initialMeta)) {
                if (nodesIds.size() > 1) {
                    requiredAllocation = nodesIds.size() - 1;
                }
            } else {
                requiredAllocation = Integer.parseInt(initialMeta);
            }
        } catch (Exception e) {
            logger.warn("failed to derived initial_meta from value {}", initialMeta);
        }

        if (nodesState.failures().length > 0) {
            for (FailedNodeException failedNodeException : nodesState.failures()) {
                logger.warn("failed to fetch state from node", failedNodeException);
            }
        }

        ObjectFloatOpenHashMap<String> indices = new ObjectFloatOpenHashMap<>();
        MetaData electedGlobalState = null;
        int found = 0;
        for (TransportNodesListGatewayMetaState.NodeLocalGatewayMetaState nodeState : nodesState) {
            if (nodeState.metaData() == null) {
                continue;
            }
            found++;
            if (electedGlobalState == null) {
                electedGlobalState = nodeState.metaData();
            } else if (nodeState.metaData().version() > electedGlobalState.version()) {
                electedGlobalState = nodeState.metaData();
            }
            for (ObjectCursor<IndexMetaData> cursor : nodeState.metaData().indices().values()) {
                indices.addTo(cursor.value.index(), 1);
            }
        }
        if (found < requiredAllocation) {
            listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");
            return;
        }
        // update the global state, and clean the indices, we elect them in the next phase
        MetaData.Builder metaDataBuilder = MetaData.builder(electedGlobalState).removeAllIndices();
        final boolean[] states = indices.allocated;
        final Object[] keys = indices.keys;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                String index = (String) keys[i];
                IndexMetaData electedIndexMetaData = null;
                int indexMetaDataCount = 0;
                for (TransportNodesListGatewayMetaState.NodeLocalGatewayMetaState nodeState : nodesState) {
                    if (nodeState.metaData() == null) {
                        continue;
                    }
                    IndexMetaData indexMetaData = nodeState.metaData().index(index);
                    if (indexMetaData == null) {
                        continue;
                    }
                    if (electedIndexMetaData == null) {
                        electedIndexMetaData = indexMetaData;
                    } else if (indexMetaData.version() > electedIndexMetaData.version()) {
                        electedIndexMetaData = indexMetaData;
                    }
                    indexMetaDataCount++;
                }
                if (electedIndexMetaData != null) {
                    if (indexMetaDataCount < requiredAllocation) {
                        logger.debug("[{}] found [{}], required [{}], not adding", index, indexMetaDataCount, requiredAllocation);
                    }
                    metaDataBuilder.put(electedIndexMetaData, false);
                }
            }
        }
        ClusterState.Builder builder = ClusterState.builder(clusterName);
        builder.metaData(metaDataBuilder);
        listener.onSuccess(builder.build());
    }

    @Override
    public Class<? extends Module> suggestIndexGateway() {
        return LocalIndexGatewayModule.class;
    }

    @Override
    public void reset() throws Exception {
        FileSystemUtils.deleteRecursively(nodeEnv.nodeDataLocations());
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        // order is important, first metaState, and then shardsState
        // so dangling indices will be recorded
        metaState.clusterChanged(event);
        shardsState.clusterChanged(event);
    }
}
