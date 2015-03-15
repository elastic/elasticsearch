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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper for filtering the index meta states when they are persisted to disk.
 */
public class IndexMetaState extends AbstractComponent {

    private final MetaStateService metaStateService;

    @Inject
    public IndexMetaState(Settings settings, MetaStateService metaStateService) {
        super(settings);
        this.metaStateService = metaStateService;
    }

    public Iterable<GatewayMetaState.IndexMetaWriteInfo> filterStateOnDataNode(ClusterChangedEvent event, MetaData currentMetaData) {
        Map<String, GatewayMetaState.IndexMetaWriteInfo> indicesToWrite = new HashMap<>();
        RoutingNode thisNode = event.state().getRoutingNodes().node(event.state().nodes().localNodeId());
        if (thisNode == null) {
            // this needs some other handling
            return indicesToWrite.values();
        }
        // iterate over all shards allocated on this node in the new cluster state but only write if ...
        for (MutableShardRouting shardRouting : thisNode) {
            IndexMetaData indexMetaData = event.state().metaData().index(shardRouting.index());
            IndexMetaData currentIndexMetaData = maybeLoadIndexState(currentMetaData, indexMetaData);
            String writeReason = null;
            // ... state persistence was disabled or index was newly created
            if (currentIndexMetaData == null) {
                writeReason = "freshly created";
                // ... new shard is allocated on node (we could optimize here and make sure only written once and not for each shard per index -> do later)
            } else if (shardRouting.initializing()) {
                writeReason = "newly allocated on node";
                // ... version changed
            } else if (indexMetaData.version() != currentIndexMetaData.version()) {
                writeReason = "version changed from [" + currentIndexMetaData.version() + "] to [" + indexMetaData.version() + "]";
            }
            if (writeReason != null) {
                indicesToWrite.put(shardRouting.index(),
                        new GatewayMetaState.IndexMetaWriteInfo(indexMetaData, currentIndexMetaData,
                                writeReason));
            }
        }
        return indicesToWrite.values();
    }

    public Iterable<GatewayMetaState.IndexMetaWriteInfo> filterStatesOnMaster(ClusterChangedEvent event, MetaData currentMetaData) {
        Map<String, GatewayMetaState.IndexMetaWriteInfo> indicesToWrite = new HashMap<>();
        MetaData newMetaData = event.state().metaData();
        // iterate over all indices but only write if ...
        for (IndexMetaData indexMetaData : newMetaData) {
            String writeReason = null;
            IndexMetaData currentIndexMetaData = maybeLoadIndexState(currentMetaData, indexMetaData);
            // ... new index or state persistence was disabled?
            if (currentIndexMetaData == null) {
                writeReason = "freshly created";
                // ... version changed
            } else if (currentIndexMetaData.version() != indexMetaData.version()) {
                writeReason = "version changed from [" + currentIndexMetaData.version() + "] to [" + indexMetaData.version() + "]";
            }
            if (writeReason != null) {
                indicesToWrite.put(indexMetaData.index(),
                        new GatewayMetaState.IndexMetaWriteInfo(indexMetaData, currentIndexMetaData,
                                writeReason));
            }

        }
        return indicesToWrite.values();
    }

    protected IndexMetaData maybeLoadIndexState(MetaData currentMetaData, IndexMetaData indexMetaData) {
        IndexMetaData currentIndexMetaData = null;
        if (currentMetaData != null) {
            currentIndexMetaData = currentMetaData.index(indexMetaData.index());
        } else {
            try {
                currentIndexMetaData = metaStateService.loadIndexState(indexMetaData.index());
            } catch (IOException e) {
                logger.debug("failed to load index state ", e);
            }
        }
        return currentIndexMetaData;
    }

}
