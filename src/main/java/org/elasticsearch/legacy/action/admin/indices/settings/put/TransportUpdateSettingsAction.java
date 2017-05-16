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

package org.elasticsearch.legacy.action.admin.indices.settings.put;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.legacy.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 *
 */
public class TransportUpdateSettingsAction extends TransportMasterNodeOperationAction<UpdateSettingsRequest, UpdateSettingsResponse> {

    private final MetaDataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         MetaDataUpdateSettingsService updateSettingsService) {
        super(settings, UpdateSettingsAction.NAME, transportService, clusterService, threadPool);
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
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
    protected void masterOperation(final UpdateSettingsRequest request, final ClusterState state, final ActionListener<UpdateSettingsResponse> listener) throws ElasticsearchException {
        final String[] concreteIndices = clusterService.state().metaData().concreteIndices(request.indicesOptions(), request.indices());
        UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest()
                .indices(concreteIndices)
                .settings(request.settings())
                .ackTimeout(request.timeout())
                .masterNodeTimeout(request.masterNodeTimeout());

        updateSettingsService.updateSettings(clusterStateUpdateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new UpdateSettingsResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to update settings on indices [{}]", t, concreteIndices);
                listener.onFailure(t);
            }
        });
    }
}
