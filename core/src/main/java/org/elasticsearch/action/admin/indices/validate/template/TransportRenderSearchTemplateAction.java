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

package org.elasticsearch.action.admin.indices.validate.template;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportRenderSearchTemplateAction extends HandledTransportAction<RenderSearchTemplateRequest, RenderSearchTemplateResponse> {

    private final ScriptService scriptService;

    @Inject
    public TransportRenderSearchTemplateAction(ScriptService scriptService, Settings settings, ThreadPool threadPool,
            TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, RenderSearchTemplateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, RenderSearchTemplateRequest::new);
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(final RenderSearchTemplateRequest request, final ActionListener<RenderSearchTemplateResponse> listener) {
        threadPool.generic().execute(new AbstractRunnable() {

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }

            @Override
            protected void doRun() throws Exception {
                ExecutableScript executable = scriptService.executable(request.template(), ScriptContext.Standard.SEARCH, request);
                BytesReference processedTemplate = (BytesReference) executable.run();
                RenderSearchTemplateResponse response = new RenderSearchTemplateResponse();
                response.source(processedTemplate);
                listener.onResponse(response);
            }
        });
    }

}
