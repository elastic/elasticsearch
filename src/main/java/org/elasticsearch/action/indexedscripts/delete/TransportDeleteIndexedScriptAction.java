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

package org.elasticsearch.action.indexedscripts.delete;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.index.IndexDeleteRequest;
import org.elasticsearch.action.delete.index.IndexDeleteResponse;
import org.elasticsearch.action.delete.index.ShardDeleteResponse;
import org.elasticsearch.action.delete.index.TransportIndexDeleteAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 */
public class TransportDeleteIndexedScriptAction extends TransportAction<DeleteIndexedScriptRequest,  DeleteIndexedScriptResponse> {

    private ScriptService scriptService;
    private Client client;

    @Inject
    public TransportDeleteIndexedScriptAction(Settings settings, ThreadPool threadPool, ScriptService scriptService,
                                              Client client, TransportService transportService) {
        super(settings, threadPool);
        this.scriptService = scriptService;
        this.client = client;
        transportService.registerHandler(DeleteIndexedScriptAction.NAME, new TransportHandler());
    }


    @Override
    protected void doExecute(final DeleteIndexedScriptRequest request, final ActionListener<DeleteIndexedScriptResponse> listener) {
        scriptService.deleteScriptFromIndex(client, request.scriptLang(), request.id(), request.version(), new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(new DeleteIndexedScriptResponse(deleteResponse.getIndex(),deleteResponse.getType(),deleteResponse.getType(),deleteResponse.getVersion(), deleteResponse.isFound()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    class TransportHandler extends BaseTransportRequestHandler<DeleteIndexedScriptRequest> {

        @Override
        public DeleteIndexedScriptRequest newInstance() {
            return new DeleteIndexedScriptRequest();
        }

        @Override
        public void messageReceived(final DeleteIndexedScriptRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<DeleteIndexedScriptResponse>() {
                @Override
                public void onResponse(DeleteIndexedScriptResponse response) {
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
                                DeleteIndexedScriptAction.NAME, request, e1);
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
