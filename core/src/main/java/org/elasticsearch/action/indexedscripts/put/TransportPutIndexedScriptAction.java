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

package org.elasticsearch.action.indexedscripts.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DelegatingActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the put indexed script operation.
 */
public class TransportPutIndexedScriptAction extends HandledTransportAction<PutIndexedScriptRequest, PutIndexedScriptResponse> {

    private final ScriptService scriptService;

    @Inject
    public TransportPutIndexedScriptAction(Settings settings, ThreadPool threadPool, ScriptService scriptService,
                                           TransportService transportService, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutIndexedScriptAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, PutIndexedScriptRequest::new);
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(final PutIndexedScriptRequest request, final ActionListener<PutIndexedScriptResponse> listener) {
        scriptService.putScriptToIndex(request, new DelegatingActionListener<IndexResponse,PutIndexedScriptResponse>(listener) {
            @Override
            public PutIndexedScriptResponse getDelegatedFromInstigator(IndexResponse indexResponse){
                return new PutIndexedScriptResponse(indexResponse.getType(),indexResponse.getId(),indexResponse.getVersion(),indexResponse.isCreated());
            }
        });
    }
}
