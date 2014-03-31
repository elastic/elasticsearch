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

package org.elasticsearch.gateway.none;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.gateway.none.NoneIndexGatewayModule;

/**
 *
 */
public class NoneGateway extends AbstractLifecycleComponent<Gateway> implements Gateway, ClusterStateListener {

    public static final String TYPE = "none";

    private final ClusterService clusterService;
    private final NodeEnvironment nodeEnv;
    private final NodeIndexDeletedAction nodeIndexDeletedAction;
    private final ClusterName clusterName;

    @Nullable
    private volatile MetaData currentMetaData;

    @Inject
    public NoneGateway(Settings settings, ClusterService clusterService, NodeEnvironment nodeEnv, NodeIndexDeletedAction nodeIndexDeletedAction, ClusterName clusterName) {
        super(settings);
        this.clusterService = clusterService;
        this.nodeEnv = nodeEnv;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.clusterName = clusterName;

        clusterService.addLast(this);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String toString() {
        return "_none_";
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public void performStateRecovery(GatewayStateRecoveredListener listener) throws GatewayException {
        logger.debug("performing state recovery");
        listener.onSuccess(ClusterState.builder(clusterName).build());
    }

    @Override
    public Class<? extends Module> suggestIndexGateway() {
        return NoneIndexGatewayModule.class;
    }

    @Override
    public void reset() {
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.currentMetaData = null;
            return;
        }

        MetaData newMetaData = event.state().metaData();

        // delete indices that were there before, but are deleted now
        // we need to do it so they won't be detected as dangling
        if (currentMetaData != null) {
            // only delete indices when we already received a state (currentMetaData != null)
            for (IndexMetaData current : currentMetaData) {
                if (!newMetaData.hasIndex(current.index())) {
                    logger.debug("[{}] deleting index that is no longer part of the metadata (indices: [{}])", current.index(), newMetaData.indices().keys());
                    if (nodeEnv.hasNodeFile()) {
                        FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(current.index())));
                    }
                    try {
                        nodeIndexDeletedAction.nodeIndexStoreDeleted(event.state(), current.index(), event.state().nodes().localNodeId());
                    } catch (Exception e) {
                        logger.debug("[{}] failed to notify master on local index store deletion", e, current.index());
                    }
                }
            }
        }

        currentMetaData = newMetaData;
    }
}
