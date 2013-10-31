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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateActionListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportClusterStateUpdateAction<CreateIndexClusterStateUpdateRequest, ClusterStateUpdateResponse, CreateIndexRequest, CreateIndexResponse> {

    private final MetaDataCreateIndexService createIndexService;

    @Inject
    public TransportCreateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataCreateIndexService createIndexService) {
        super(settings, transportService, clusterService, threadPool);
        this.createIndexService = createIndexService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return CreateIndexAction.NAME;
    }

    @Override
    protected CreateIndexRequest newRequest() {
        return new CreateIndexRequest();
    }

    @Override
    protected CreateIndexResponse newResponse() {
        return new CreateIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, request.index());
    }

    @Override
    protected CreateIndexClusterStateUpdateRequest newClusterStateUpdateRequest(CreateIndexRequest acknowledgedRequest) {
        String cause = Strings.hasLength(acknowledgedRequest.cause()) ? acknowledgedRequest.cause() : "api";
        return new CreateIndexClusterStateUpdateRequest(cause, acknowledgedRequest.index())
                .settings(acknowledgedRequest.settings()).mappings(acknowledgedRequest.mappings())
                .customs(acknowledgedRequest.customs());
    }

    @Override
    protected CreateIndexResponse newResponse(ClusterStateUpdateResponse updateResponse) {
        return new CreateIndexResponse(updateResponse.isAcknowledged());
    }

    @Override
    protected ClusterStateUpdateActionListener<ClusterStateUpdateResponse, CreateIndexResponse> newClusterStateUpdateActionListener(final CreateIndexClusterStateUpdateRequest request, final ActionListener<CreateIndexResponse> listener) {
        return new ClusterStateUpdateActionListener<ClusterStateUpdateResponse, CreateIndexResponse>(listener) {
            @Override
            protected CreateIndexResponse newResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                return TransportCreateIndexAction.this.newResponse(clusterStateUpdateResponse);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof IndexAlreadyExistsException) {
                    logger.trace("[{}] failed to create", t, request.index());
                } else {
                    logger.debug("[{}] failed to create", t, request.index());
                }
                listener.onFailure(t);
            }
        };
    }

    @Override
    protected void updateClusterState(CreateIndexClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterStateUpdateResponse, CreateIndexResponse> listener) {
        createIndexService.createIndex(updateRequest, listener);
    }
}
