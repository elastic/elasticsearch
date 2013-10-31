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

package org.elasticsearch.action.admin.indices.settings;

import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.action.support.master.ClusterStateUpdateActionListener;
import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Update index settings action
 */
public class TransportUpdateSettingsAction extends TransportClusterStateUpdateAction<UpdateSettingsClusterStateUpdateRequest, ClusterStateUpdateResponse, UpdateSettingsRequest, UpdateSettingsResponse> {

    private final MetaDataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         MetaDataUpdateSettingsService updateSettingsService) {
        super(settings, transportService, clusterService, threadPool);
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return UpdateSettingsAction.NAME;
    }

    @Override
    protected UpdateSettingsRequest newRequest() {
        return new UpdateSettingsRequest();
    }

    @Override
    protected UpdateSettingsResponse newResponse() {
        return new UpdateSettingsResponse();
    }

    @Override
    protected UpdateSettingsResponse newResponse(ClusterStateUpdateResponse updateResponse) {
        return new UpdateSettingsResponse(updateResponse.isAcknowledged());
    }

    @Override
    protected UpdateSettingsClusterStateUpdateRequest newClusterStateUpdateRequest(UpdateSettingsRequest acknowledgedRequest) {
        return new UpdateSettingsClusterStateUpdateRequest(acknowledgedRequest.indices(), acknowledgedRequest.settings());
    }

    @Override
    protected void updateClusterState(UpdateSettingsClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterStateUpdateResponse, UpdateSettingsResponse> listener) {
        updateSettingsService.updateSettings(updateRequest, listener);
    }
}
