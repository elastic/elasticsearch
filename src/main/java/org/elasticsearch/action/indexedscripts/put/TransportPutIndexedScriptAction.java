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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the index operation.
 * <p/>
 * <p>Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to <tt>true</tt>, will automatically create an index if one does not exists.
 * Defaults to <tt>true</tt>.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to <tt>true</tt>.
 * </ul>
 */
public class TransportPutIndexedScriptAction extends TransportAction<PutIndexedScriptRequest, PutIndexedScriptResponse> {

    private ScriptService scriptService;
    private Client client;

    @Inject
    public TransportPutIndexedScriptAction(Settings settings, ThreadPool threadPool,
                                           ScriptService scriptService, Client client,
                                           TransportService transportService) {
        super(settings,threadPool);
        this.client = client;
        this.scriptService = scriptService;
        transportService.registerHandler(PutIndexedScriptAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final PutIndexedScriptRequest request, final ActionListener<PutIndexedScriptResponse> listener) {
        try {
            scriptService.putScriptToIndex(client, request.safeSource(), request.scriptLang(), request.id(), null, request.opType().toString(), request.version(), request.versionType(), new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(new PutIndexedScriptResponse(indexResponse.getType(),indexResponse.getId(),indexResponse.getVersion(),indexResponse.isCreated()));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } catch (IOException ioe) {
            listener.onFailure(ioe);
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<PutIndexedScriptRequest> {

        @Override
        public PutIndexedScriptRequest newInstance() {
            return new PutIndexedScriptRequest();
        }

        @Override
        public void messageReceived(final PutIndexedScriptRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<PutIndexedScriptResponse>() {
                @Override
                public void onResponse(PutIndexedScriptResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [{}] and request [{}]",
                                PutIndexedScriptAction.NAME, request, e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

}
