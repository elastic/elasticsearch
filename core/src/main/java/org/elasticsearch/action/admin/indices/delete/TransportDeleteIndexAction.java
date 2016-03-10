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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportDeleteIndexAction extends TransportMasterNodeAction<DeleteIndexRequest, DeleteIndexResponse> {

    private final MetaDataDeleteIndexService deleteIndexService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataDeleteIndexService deleteIndexService,
                                      NodeSettingsService nodeSettingsService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, DestructiveOperations destructiveOperations) {
        super(settings, DeleteIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DeleteIndexRequest.class);
        this.deleteIndexService = deleteIndexService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteIndexResponse newResponse() {
        return new DeleteIndexResponse();
    }

    @Override
    protected void doExecute(Task task, DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndices(state, request));
    }

    @Override
    protected void masterOperation(final DeleteIndexRequest request, final ClusterState state, final ActionListener<DeleteIndexResponse> listener) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices.length == 0) {
            listener.onResponse(new DeleteIndexResponse(true));
            return;
        }
        deleteIndexService.deleteIndices(new MetaDataDeleteIndexService.Request(concreteIndices).timeout(request.timeout()).masterTimeout(request.masterNodeTimeout()), new MetaDataDeleteIndexService.Listener() {

            @Override
            public void onResponse(MetaDataDeleteIndexService.Response response) {
                listener.onResponse(new DeleteIndexResponse(response.acknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }
}
