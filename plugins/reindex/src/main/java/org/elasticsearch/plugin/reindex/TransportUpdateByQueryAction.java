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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkIndexByScrollResponse> {
    private final Client client;
    private final ScriptService scriptService;

    @Inject
    public TransportUpdateByQueryAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, Client client, TransportService transportService,
            ScriptService scriptService) {
        super(settings, UpdateByQueryAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, UpdateByQueryRequest::new);
        this.client = client;
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(UpdateByQueryRequest request,
            ActionListener<BulkIndexByScrollResponse> listener) {
        new AsyncIndexBySearchAction(request, listener).start();
    }

    /**
     * Simple implementation of index-by-search scrolling and bulk. There are
     * tons of optimizations that can be done on certain types of index-by-query
     * requests but this makes no attempt to do any of them so it can be as
     * simple possible.
     */
    class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<UpdateByQueryRequest, BulkIndexByScrollResponse> {
        public AsyncIndexBySearchAction(UpdateByQueryRequest request,
                ActionListener<BulkIndexByScrollResponse> listener) {
            super(logger, scriptService, client, request, request.source(),
                    listener);
        }

        @Override
        protected IndexRequest buildIndexRequest(SearchHit doc) {
            IndexRequest index = new IndexRequest(mainRequest);
            index.index(doc.index());
            index.type(doc.type());
            index.id(doc.id());
            index.source(doc.sourceRef());
            index.versionType(VersionType.INTERNAL);
            index.version(doc.version());
            return index;
        }

        @Override
        protected BulkIndexByScrollResponse buildResponse(long took) {
            return new BulkIndexByScrollResponse(took, updated(), batches(), versionConflicts(), noops(), failures());
        }
    }
}
