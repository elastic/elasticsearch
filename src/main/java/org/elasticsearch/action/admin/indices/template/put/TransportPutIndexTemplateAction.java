/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.support.master.ClusterStateUpdateActionListener;
import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Put index template action.
 */
public class TransportPutIndexTemplateAction extends TransportClusterStateUpdateAction<PutTemplateClusterStateUpdateRequest, ClusterStateUpdateResponse, PutIndexTemplateRequest, PutIndexTemplateResponse> {

    private final MetaDataIndexTemplateService indexTemplateService;

    @Inject
    public TransportPutIndexTemplateAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, MetaDataIndexTemplateService indexTemplateService) {
        super(settings, transportService, clusterService, threadPool);
        this.indexTemplateService = indexTemplateService;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return PutIndexTemplateAction.NAME;
    }

    @Override
    protected PutIndexTemplateRequest newRequest() {
        return new PutIndexTemplateRequest();
    }

    @Override
    protected PutIndexTemplateResponse newResponse() {
        return new PutIndexTemplateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected PutTemplateClusterStateUpdateRequest newClusterStateUpdateRequest(PutIndexTemplateRequest acknowledgedRequest) {
        String cause = Strings.hasLength(acknowledgedRequest.cause()) ? acknowledgedRequest.cause() : "api";
        return new PutTemplateClusterStateUpdateRequest(acknowledgedRequest.name(), cause)
                .template(acknowledgedRequest.template()).order(acknowledgedRequest.order())
                .settings(acknowledgedRequest.settings()).mappings(acknowledgedRequest.mappings())
                .customs(acknowledgedRequest.customs()).create(acknowledgedRequest.create());
    }

    @Override
    protected PutIndexTemplateResponse newResponse(ClusterStateUpdateResponse updateResponse) {
        return new PutIndexTemplateResponse(updateResponse.isAcknowledged());
    }

    @Override
    protected void updateClusterState(PutTemplateClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterStateUpdateResponse, PutIndexTemplateResponse> listener) {
        indexTemplateService.putTemplate(updateRequest, listener);
    }
}
