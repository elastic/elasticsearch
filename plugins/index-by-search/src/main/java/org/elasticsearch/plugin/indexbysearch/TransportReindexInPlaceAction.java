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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportReindexInPlaceAction
        extends HandledTransportAction<ReindexInPlaceRequest, BulkIndexByScrollResponse> {
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final TransportBulkAction bulkAction;
    private final TransportClearScrollAction clearScrollAction;

    @Inject
    public TransportReindexInPlaceAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, TransportSearchAction transportSearchAction,
            TransportSearchScrollAction transportSearchScrollAction, TransportBulkAction bulkAction,
            TransportClearScrollAction clearScrollAction, TransportService transportService) {
        super(settings, ReindexInPlaceAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, ReindexInPlaceRequest::new);
        this.searchAction = transportSearchAction;
        this.scrollAction = transportSearchScrollAction;
        this.bulkAction = bulkAction;
        this.clearScrollAction = clearScrollAction;
    }

    @Override
    protected void doExecute(ReindexInPlaceRequest request,
            ActionListener<BulkIndexByScrollResponse> listener) {
        new AsyncIndexBySearchAction(request, listener).start();
    }

    /**
     * Simple implementation of index-by-search scrolling and bulk. There are
     * tons of optimizations that can be done on certain types of index-by-query
     * requests but this makes no attempt to do any of them so it can be as
     * simple possible.
     */
    class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<ReindexInPlaceRequest, BulkIndexByScrollResponse> {
        public AsyncIndexBySearchAction(ReindexInPlaceRequest request,
                ActionListener<BulkIndexByScrollResponse> listener) {
            super(logger, searchAction, scrollAction, bulkAction, clearScrollAction, request, request.search(),
                    listener);
        }

        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            BulkRequest bulkRequest = new BulkRequest(mainRequest);

            for (SearchHit doc : docs) {
                IndexRequest index = new IndexRequest(mainRequest);

                index.index(doc.index());
                index.type(doc.type());
                index.id(doc.id());
                index.source(doc.sourceRef());
                index.versionType(mainRequest.useReindexVersionType() ? VersionType.REINDEX : VersionType.INTERNAL);
                index.version(doc.version());

                copyMetadata(index, doc);

                bulkRequest.add(index);
            }
            return bulkRequest;
        }

        @Override
        protected BulkIndexByScrollResponse buildResponse(long took) {
            return new BulkIndexByScrollResponse(took, updated(), batches(), versionConflicts(), failures());
        }
    }
}
