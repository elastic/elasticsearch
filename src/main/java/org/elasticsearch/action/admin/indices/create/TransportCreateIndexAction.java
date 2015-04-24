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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeOperationAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetaDataCreateIndexService createIndexService;

    @Inject
    public TransportCreateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataCreateIndexService createIndexService, ActionFilters actionFilters) {
        super(settings, CreateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest.class);
        this.createIndexService = createIndexService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse newResponse() {
        return new CreateIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void masterOperation(final CreateIndexRequest request, final ClusterState state, final ActionListener<CreateIndexResponse> listener) throws ElasticsearchException {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(request, cause, request.index())
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .settings(request.settings()).mappings(request.mappings())
                .aliases(request.aliases()).customs(request.customs());

        createIndexService.createIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new CreateIndexResponse(response.isAcknowledged()));
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
        });
    }
}
