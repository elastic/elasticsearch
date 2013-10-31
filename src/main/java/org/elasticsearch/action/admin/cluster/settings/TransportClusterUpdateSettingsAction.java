/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.action.support.master.ClusterStateUpdateActionListener;
import org.elasticsearch.cluster.metadata.MetaDataClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Update cluster settings action
 */
public class TransportClusterUpdateSettingsAction extends TransportClusterStateUpdateAction<ClusterUpdateSettingsClusterStateUpdateRequest, ClusterUpdateSettingsClusterStateUpdateResponse, ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private final MetaDataClusterService metaDataClusterService;

    @Inject
    public TransportClusterUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, MetaDataClusterService metaDataClusterService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataClusterService = metaDataClusterService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return ClusterUpdateSettingsAction.NAME;
    }

    @Override
    protected ClusterUpdateSettingsRequest newRequest() {
        return new ClusterUpdateSettingsRequest();
    }

    @Override
    protected ClusterUpdateSettingsResponse newResponse() {
        return new ClusterUpdateSettingsResponse();
    }

    @Override
    protected ClusterUpdateSettingsResponse newResponse(ClusterUpdateSettingsClusterStateUpdateResponse updateResponse) {
        return new ClusterUpdateSettingsResponse(updateResponse.isAcknowledged(), updateResponse.transientSettings(), updateResponse.persistentSettings());
    }

    @Override
    protected ClusterUpdateSettingsClusterStateUpdateRequest newClusterStateUpdateRequest(ClusterUpdateSettingsRequest acknowledgedRequest) {
        return new ClusterUpdateSettingsClusterStateUpdateRequest().transientSettings(acknowledgedRequest.transientSettings())
                .persistentSettings(acknowledgedRequest.persistentSettings());
    }

    @Override
    protected void updateClusterState(ClusterUpdateSettingsClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterUpdateSettingsClusterStateUpdateResponse, ClusterUpdateSettingsResponse> listener) {
        metaDataClusterService.updateSettings(updateRequest, listener);
    }
}
