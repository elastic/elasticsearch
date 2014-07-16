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

package org.elasticsearch.action.indexedscripts.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the get operation.
 */
public class TransportGetIndexedScriptAction extends HandledTransportAction<GetIndexedScriptRequest, GetIndexedScriptResponse> {

    public static final boolean REFRESH_FORCE = false;
    ScriptService scriptService;
    Client client;

    @Inject
    public TransportGetIndexedScriptAction(Settings settings, ThreadPool threadPool, ScriptService scriptService,
                                           TransportService transportService, Client client) {
        super(settings, GetIndexedScriptAction.NAME, threadPool,transportService);
        this.scriptService = scriptService;
        this.client = client;
    }

    @Override
    public GetIndexedScriptRequest newRequestInstance(){
        return new GetIndexedScriptRequest();
    }

    @Override
    public void doExecute(GetIndexedScriptRequest request, ActionListener<GetIndexedScriptResponse> listener){
        try {
            GetResponse scriptResponse = scriptService.queryScriptIndex(client, request.scriptLang(), request.id(), request.version(), request.versionType());
            listener.onResponse(new GetIndexedScriptResponse(scriptResponse));
        } catch(Throwable e){
            listener.onFailure(e);
        }
    }
}
