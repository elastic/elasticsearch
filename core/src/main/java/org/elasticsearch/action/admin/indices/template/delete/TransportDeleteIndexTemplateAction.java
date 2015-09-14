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
package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportDeleteIndexTemplateAction extends TransportMasterNodeAction<DeleteIndexTemplateRequest, DeleteIndexTemplateResponse> {

    private final MetaDataIndexTemplateService indexTemplateService;

    @Inject
    public TransportDeleteIndexTemplateAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                              ThreadPool threadPool, MetaDataIndexTemplateService indexTemplateService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DeleteIndexTemplateRequest::new);
        this.indexTemplateService = indexTemplateService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteIndexTemplateResponse newResponse() {
        return new DeleteIndexTemplateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexTemplateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, "");
    }

    @Override
    protected void masterOperation(final DeleteIndexTemplateRequest request, final ClusterState state, final ActionListener<DeleteIndexTemplateResponse> listener) {
        indexTemplateService.removeTemplates(new MetaDataIndexTemplateService.RemoveRequest(request.name()).masterTimeout(request.masterNodeTimeout()), new MetaDataIndexTemplateService.RemoveListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.RemoveResponse response) {
                listener.onResponse(new DeleteIndexTemplateResponse(response.acknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to delete templates [{}]", t, request.name());
                listener.onFailure(t);
            }
        });
    }
}
