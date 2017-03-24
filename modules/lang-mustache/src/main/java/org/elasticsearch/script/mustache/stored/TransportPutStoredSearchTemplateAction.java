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
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.script.TemplateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutStoredSearchTemplateAction extends TransportMasterNodeAction<
        PutStoredTemplateRequest, PutStoredTemplateResponse> {

    private final TemplateService templateService;
    private final int maxScriptSizeInBytes;

    @Inject
    public TransportPutStoredSearchTemplateAction(Settings settings,
            TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
            TemplateService templateService) {
        super(settings, PutStoredTemplateAction.NAME, transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver,
                PutStoredTemplateRequest::new);
        this.templateService = templateService;
        maxScriptSizeInBytes = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(settings);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutStoredTemplateResponse newResponse() {
        return new PutStoredTemplateResponse();
    }

    @Override
    protected void masterOperation(PutStoredTemplateRequest request, ClusterState state,
            ActionListener<PutStoredTemplateResponse> listener) throws Exception {
        if (request.content().length() > maxScriptSizeInBytes) {
            throw new IllegalArgumentException("exceeded max allowed stored script size in bytes ["
                    + maxScriptSizeInBytes + "] with size [" + request.content().length()
                    + "] for script [" + request.id() + "]");
        }

        StoredScriptSource source = StoredScriptSource.parse(templateService.getTemplateLanguage(),
                request.content(), request.xContentType());
        try {
            templateService.checkCompileBeforeStore(source);
        } catch (IllegalArgumentException | ScriptException e) {
            throw new IllegalArgumentException("failed to parse/compile stored search template ["
                    + request.id() + "]"
                    + (source.getCode() == null ? "" : " using code [" + source.getCode() + "]"),
                    e);
        }

        clusterService.submitStateUpdateTask("put-search-template-" + request.id(),
                new AckedClusterStateUpdateTask<PutStoredTemplateResponse>(request,
                        listener) {

                    @Override
                    protected PutStoredTemplateResponse newResponse(boolean acknowledged) {
                        return new PutStoredTemplateResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ScriptMetaData smd = currentState.metaData().custom(ScriptMetaData.TYPE);
                        smd = ScriptMetaData.putStoredScript(smd, request.id(), source);
                        MetaData.Builder mdb = MetaData.builder(currentState.getMetaData())
                                .putCustom(ScriptMetaData.TYPE, smd);

                        return ClusterState.builder(currentState).metaData(mdb).build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(PutStoredTemplateRequest request,
            ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
