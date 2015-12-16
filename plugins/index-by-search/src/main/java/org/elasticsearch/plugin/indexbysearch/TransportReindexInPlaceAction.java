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

package org.elasticsearch.plugin.indexbysearch;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportReindexInPlaceAction
        extends HandledTransportAction<ReindexInPlaceRequest, BulkIndexByScrollResponse> {
    private final Client client;
    private final ScriptService scriptService;

    @Inject
    public TransportReindexInPlaceAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, Client client, TransportService transportService,
            ScriptService scriptService) {
        super(settings, ReindexInPlaceAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, ReindexInPlaceRequest::new);
        this.client = client;
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(ReindexInPlaceRequest request,
            ActionListener<BulkIndexByScrollResponse> listener) {
        new AsyncIndexBySearchAction(request, listener, scriptService).start();
    }

    /**
     * Simple implementation of index-by-search scrolling and bulk. There are
     * tons of optimizations that can be done on certain types of index-by-query
     * requests but this makes no attempt to do any of them so it can be as
     * simple possible.
     */
    class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<ReindexInPlaceRequest, BulkIndexByScrollResponse> {
        private final ScriptService scriptService;
        private final CompiledScript script;
        public AsyncIndexBySearchAction(ReindexInPlaceRequest request,
                ActionListener<BulkIndexByScrollResponse> listener, ScriptService scriptService) {
            super(logger, client, request, request.source(),
                    listener);
            this.scriptService = scriptService;
            if (request.script() == null) {
                script = null;
            } else {
                script = scriptService.compile(request.script(), ScriptContext.Standard.UPDATE, request);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            BulkRequest bulkRequest = new BulkRequest(mainRequest);
            ExecutableScript executable = null;
            Map<String, Object> scriptCtx = null;

            for (SearchHit doc : docs) {
                IndexRequest index = new IndexRequest(mainRequest);

                index.index(doc.index());
                index.type(doc.type());
                index.id(doc.id());
                index.source(doc.sourceRef());
                index.versionType(mainRequest.versionType().versionType(mainRequest));
                index.version(doc.version());

                copyMetadata(index, doc);

                if (script != null) {
                    if (executable == null) {
                        executable = scriptService.executable(script, mainRequest.script().getParams());
                        scriptCtx = new HashMap<>(2);
                    }
                    Map<String, Object> source = index.sourceAsMap();
                    scriptCtx.put("_source", source);
                    scriptCtx.put("op", "update");
                    executable.setNextVar("ctx", scriptCtx);
                    executable.run();
                    scriptCtx = (Map<String, Object>) executable.unwrap(scriptCtx);
                    String newOp = (String) scriptCtx.get("op");
                    if (newOp == null) {
                        throw new IllegalArgumentException("Script cleared op!");
                    }
                    if ("noop".equals(newOp)) {
                        countNoop();
                        continue;
                    }
                    if ("update".equals(newOp) == false) {
                        throw new IllegalArgumentException("Invalid op [" + newOp + ']');
                    }

                    /*
                     * It'd be lovely to only set the source if we know its been
                     * modified but it isn't worth keeping two copies of it
                     * around just to check!
                     */
                    Map<String, Object> newSource = (Map<String, Object>) scriptCtx.get("_source");
                    index.source(newSource);
                }
                bulkRequest.add(index);
            }
            return bulkRequest;
        }

        @Override
        protected BulkIndexByScrollResponse buildResponse(long took) {
            return new BulkIndexByScrollResponse(took, updated(), batches(), versionConflicts(), noops(), failures());
        }
    }
}
