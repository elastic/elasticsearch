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

package org.elasticsearch.legacy.action.indexedscripts.put;

import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.DelegatingActionListener;
import org.elasticsearch.legacy.action.index.IndexResponse;
import org.elasticsearch.legacy.action.support.HandledTransportAction;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.script.ScriptService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 * Performs the put indexed script operation.
 */
public class TransportPutIndexedScriptAction extends HandledTransportAction<PutIndexedScriptRequest, PutIndexedScriptResponse> {

    private ScriptService scriptService;
    private Client client;

    @Inject
    public TransportPutIndexedScriptAction(Settings settings, ThreadPool threadPool,
                                           ScriptService scriptService, Client client,
                                           TransportService transportService) {
        super(settings, PutIndexedScriptAction.NAME, threadPool, transportService);
        this.client = client;
        this.scriptService = scriptService;
    }

    @Override
    public PutIndexedScriptRequest newRequestInstance(){
        return new PutIndexedScriptRequest();
    }

    @Override
    protected void doExecute(final PutIndexedScriptRequest request, final ActionListener<PutIndexedScriptResponse> listener) {
        scriptService.putScriptToIndex(client, request.safeSource(), request.scriptLang(), request.id(), null, request.opType().toString(), request.version(), request.versionType(), new DelegatingActionListener<IndexResponse,PutIndexedScriptResponse>(listener) {
            @Override
                public PutIndexedScriptResponse getDelegatedFromInstigator(IndexResponse indexResponse){
                return new PutIndexedScriptResponse(indexResponse.getType(),indexResponse.getId(),indexResponse.getVersion(),indexResponse.isCreated());
            }
        });
    }

}
