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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Gateway extends AbstractComponent implements ClusterStateApplier {

    private final ClusterService clusterService;

    private final GatewayMetaState metaState;

    private final TransportNodesListGatewayMetaState listGatewayMetaState;

    private final Supplier<Integer> minimumMasterNodesProvider;
    private final IndicesService indicesService;

    public Gateway(Settings settings, ClusterService clusterService, GatewayMetaState metaState,
                   TransportNodesListGatewayMetaState listGatewayMetaState, Discovery discovery,
                   IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.metaState = metaState;
        this.listGatewayMetaState = listGatewayMetaState;
        this.minimumMasterNodesProvider = discovery::getMinimumMasterNodes;
        clusterService.addLowPriorityApplier(this);
    }

    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        String[] nodesIds = clusterService.state().nodes().getMasterNodes().keys().toArray(String.class);
        logger.trace("performing state recovery from {}", Arrays.toString(nodesIds));
        TransportNodesListGatewayMetaState.NodesGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds, null).actionGet();


        int requiredAllocation = Math.max(1, minimumMasterNodesProvider.get());


        if (nodesState.hasFailures()) {
            for (FailedNodeException failedNodeException : nodesState.failures()) {
                logger.warn("failed to fetch state from node", failedNodeException);
            }
        }

        ObjectFloatHashMap<Index> indices = new ObjectFloatHashMap<>();
        MetaData electedGlobalState = null;
        int found = 0;
        for (TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
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
                indices.addTo(cursor.value.getIndex(), 1);
            }
        }
        if (found < requiredAllocation) {
            listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");
            return;
        }
        // update the global state, and clean the indices, we elect them in the next phase
        MetaData.Builder metaDataBuilder = MetaData.builder(electedGlobalState).removeAllIndices();

        assert !indices.containsKey(null);
        final Object[] keys = indices.keys;
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                Index index = (Index) keys[i];
                IndexMetaData electedIndexMetaData = null;
                int indexMetaDataCount = 0;
                for (TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
                    if (nodeState.metaData() == null) {
                        continue;
                    }
                    IndexMetaData indexMetaData = nodeState.metaData().index(index);
                    if (indexMetaData == null) {
                        continue;
                    }
                    if (electedIndexMetaData == null) {
                        electedIndexMetaData = indexMetaData;
                    } else if (indexMetaData.getVersion() > electedIndexMetaData.getVersion()) {
                        electedIndexMetaData = indexMetaData;
                    }
                    indexMetaDataCount++;
                }
                if (electedIndexMetaData != null) {
                    if (indexMetaDataCount < requiredAllocation) {
                        logger.debug("[{}] found [{}], required [{}], not adding", index, indexMetaDataCount, requiredAllocation);
                        
                        //if we have 5 nodes cluster(such as 1,2,3,4,5 node),when 5 node leave the cluster,and now cluster delete one index.
                        //then restart all nodes,if 1,2,5 join and find master,then 3,4 join the cluster.After started, this cluster state is red,
                        //because the delete index(lose some shards) is still part of the cluster metedata.
                        //to avoid this problem,this check the tomstones and indices,if tomston has index which is same in indices,
                        //don't put index in metedata                  
                        List<IndexGraveyard.Tombstone> tombstones = electedGlobalState.indexGraveyard().getTombstones();
                        List<Index> deleteIndexList = tombstones.stream().map(IndexGraveyard.Tombstone::getIndex).collect(Collectors.toList());
                        boolean isAlreadyDelete = false;
                        for (Index deleteIndex : deleteIndexList) {
                            if (deleteIndex.getName().equals(electedIndexMetaData.getIndex().getName()) &&
                                deleteIndex.getUUID().equals(electedIndexMetaData.getIndexUUID())) {
                                logger.trace("[{}] index delete in some node but not in allNode, so need't put into metaData", index.getName());
                                isAlreadyDelete = true;
                                break;
                            }
                        }
                        if (isAlreadyDelete == true) {
                            continue;
                        }
                    } // TODO if this logging statement is correct then we are missing an else here
                    try {
                        if (electedIndexMetaData.getState() == IndexMetaData.State.OPEN) {
                            // verify that we can actually create this index - if not we recover it as closed with lots of warn logs
                            indicesService.verifyIndexMetadata(electedIndexMetaData, electedIndexMetaData);
                        }
                    } catch (Exception e) {
                        final Index electedIndex = electedIndexMetaData.getIndex();
                        logger.warn(
                            (org.apache.logging.log4j.util.Supplier<?>)
                                () -> new ParameterizedMessage("recovering index {} failed - recovering as closed", electedIndex), e);
                        electedIndexMetaData = IndexMetaData.builder(electedIndexMetaData).state(IndexMetaData.State.CLOSE).build();
                    }

                    metaDataBuilder.put(electedIndexMetaData, false);
                }
            }
        }
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        metaDataBuilder.persistentSettings(
            clusterSettings.archiveUnknownOrInvalidSettings(
                metaDataBuilder.persistentSettings(),
                e -> logUnknownSetting("persistent", e),
                (e, ex) -> logInvalidSetting("persistent", e, ex)));
        metaDataBuilder.transientSettings(
            clusterSettings.archiveUnknownOrInvalidSettings(
                metaDataBuilder.transientSettings(),
                e -> logUnknownSetting("transient", e),
                (e, ex) -> logInvalidSetting("transient", e, ex)));
        ClusterState.Builder builder = ClusterState.builder(clusterService.getClusterName());
        builder.metaData(metaDataBuilder);
        listener.onSuccess(builder.build());
    }

    private void logUnknownSetting(String settingType, Map.Entry<String, String> e) {
        logger.warn("ignoring unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private void logInvalidSetting(String settingType, Map.Entry<String, String> e, IllegalArgumentException ex) {
        logger.warn(
            (org.apache.logging.log4j.util.Supplier<?>)
                () -> new ParameterizedMessage("ignoring invalid {} setting: [{}] with value [{}]; archiving",
                    settingType,
                    e.getKey(),
                    e.getValue()),
            ex);
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        // order is important, first metaState, and then shardsState
        // so dangling indices will be recorded
        metaState.applyClusterState(event);
    }

    public interface GatewayStateRecoveredListener {
        void onSuccess(ClusterState build);

        void onFailure(String s);
    }
}
