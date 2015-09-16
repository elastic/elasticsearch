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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;

/**
 * Close index action
 */
public class TransportCloseIndexAction extends TransportMasterNodeAction<CloseIndexRequest, CloseIndexResponse> {

    private final MetaDataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportCloseIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataIndexStateService indexStateService,
                                     NodeSettingsService nodeSettingsService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, DestructiveOperations destructiveOperations) {
        super(settings, CloseIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, CloseIndexRequest::new);
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CloseIndexResponse newResponse() {
        return new CloseIndexResponse();
    }

    @Override
    protected void doExecute(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndices(state, request));
    }

    @Override
    protected void masterOperation(final CloseIndexRequest request, final ClusterState state, final ActionListener<CloseIndexResponse> listener) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        CloseIndexClusterStateUpdateRequest updateRequest = new CloseIndexClusterStateUpdateRequest()
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .indices(concreteIndices);

        indexStateService.closeIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new CloseIndexResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to close indices [{}]", t, (Object)concreteIndices);
                listener.onFailure(t);
            }
        });
    }
}
