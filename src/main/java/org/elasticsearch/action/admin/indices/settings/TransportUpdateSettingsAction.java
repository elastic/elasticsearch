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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportUpdateSettingsAction extends TransportMasterNodeOperationAction<UpdateSettingsRequest, UpdateSettingsResponse> {

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
    protected void masterOperation(final UpdateSettingsRequest request, final ClusterState state, final ActionListener<UpdateSettingsResponse> listener) throws ElasticSearchException {

        UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest()
                .indices(request.indices())
                .settings(request.settings())
                .ackTimeout(request.timeout())
                .masterNodeTimeout(request.masterNodeTimeout());

        updateSettingsService.updateSettings(clusterStateUpdateRequest, new ClusterStateUpdateListener() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new UpdateSettingsResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to update settings on indices [{}]", t, request.indices());
                listener.onFailure(t);
            }
        });
    }
}
