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

package org.elasticsearch.script.mustache.stored;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.script.TemplateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteStoredSearchTemplateAction extends TransportMasterNodeAction<
        DeleteStoredSearchTemplateRequest, DeleteStoredSearchTemplateResponse> {

    private final TemplateService templateService;

    @Inject
    public TransportDeleteStoredSearchTemplateAction(Settings settings,
            TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
            TemplateService templateService) {
        super(settings, DeleteStoredSearchTemplateAction.NAME, transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver,
                DeleteStoredSearchTemplateRequest::new);
        this.templateService = templateService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteStoredSearchTemplateResponse newResponse() {
        return new DeleteStoredSearchTemplateResponse();
    }

    @Override
    protected void masterOperation(DeleteStoredSearchTemplateRequest request, ClusterState state,
            ActionListener<DeleteStoredSearchTemplateResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("delete-search-template-" + request.id(),
                new AckedClusterStateUpdateTask<DeleteStoredSearchTemplateResponse>(request,
                        listener) {
                    @Override
                    protected DeleteStoredSearchTemplateResponse newResponse(boolean acknowledged) {
                        return new DeleteStoredSearchTemplateResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ScriptMetaData smd = currentState.metaData().custom(ScriptMetaData.TYPE);
                        smd = ScriptMetaData.deleteStoredScript(smd, request.id(),
                                templateService.getTemplateLanguage());
                        MetaData.Builder mdb = MetaData.builder(currentState.getMetaData())
                                .putCustom(ScriptMetaData.TYPE, smd);
                        return ClusterState.builder(currentState).metaData(mdb).build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteStoredSearchTemplateRequest request,
            ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
