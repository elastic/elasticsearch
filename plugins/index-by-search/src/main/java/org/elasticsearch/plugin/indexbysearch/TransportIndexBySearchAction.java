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
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportIndexBySearchAction extends HandledTransportAction<IndexBySearchRequest, IndexBySearchResponse> {
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final TransportBulkAction bulkAction;
    private final TransportClearScrollAction clearScrollAction;

    @Inject
    public TransportIndexBySearchAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, TransportSearchAction transportSearchAction,
            TransportSearchScrollAction transportSearchScrollAction, TransportBulkAction bulkAction,
            TransportClearScrollAction clearScrollAction, TransportService transportService) {
        super(settings, IndexBySearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                IndexBySearchRequest::new);
        this.searchAction = transportSearchAction;
        this.scrollAction = transportSearchScrollAction;
        this.bulkAction = bulkAction;
        this.clearScrollAction = clearScrollAction;
    }

    @Override
    protected void doExecute(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
        new AsyncIndexBySearchAction(request, listener).start();
    }



    /**
     * Simple implementation of index-by-search scrolling and bulk. There are
     * tons of optimizations that can be done on certain types of index-by-query
     * requests but this makes no attempt to do any of them so it can be as
     * simple possible.
     */
    class AsyncIndexBySearchAction extends AbstractAsyncScrollAction<IndexBySearchRequest, IndexBySearchResponse> {
        public AsyncIndexBySearchAction(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
            super(logger, searchAction, scrollAction, bulkAction, clearScrollAction, request, request.search(), listener);
        }

        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            BulkRequest bulkRequest = new BulkRequest(mainRequest);

            for (SearchHit doc : docs) {
                IndexRequest index = new IndexRequest(mainRequest.index(), mainRequest);

                // We want the index from the copied request, not the doc.
                index.id(doc.id());
                if (index.type() == null) {
                    /*
                     * Default to doc's type if not specified in request so its
                     * easy to do a scripted update.
                     */
                    index.type(doc.type());
                }
                index.source(doc.sourceRef());
                if (index.version() == Versions.MATCH_ANY /* The default */) {
                    index.version(doc.version());
                } else if (index.version() == Versions.NOT_SET) {
                    /*
                     * We borrow NOT_SET here to mean
                     * "don't set the version parameter" so we set it back to
                     * the default.
                     */
                    index.version(Versions.MATCH_ANY);
                }

                SearchHitField parent = doc.field("_parent");
                if (parent != null) {
                    index.parent(parent.value());
                }
                handleRouting(doc, index);
                SearchHitField timestamp = doc.field("_timestamp");
                if (timestamp != null) {
                    // Comes back as a Long but needs to be a string
                    index.timestamp(timestamp.value().toString());
                }
                SearchHitField ttl = doc.field("_ttl");
                if (ttl != null) {
                    index.ttl(ttl.value());
                }

                bulkRequest.add(index);
            }
            return bulkRequest;
        }

        private void handleRouting(SearchHit doc, IndexRequest index) {
            String routingSpec = mainRequest.index().routing();
            if (routingSpec == null) {
                copyRouting(doc, index);
                return;
            }
            if (routingSpec.startsWith("=")) {
                index.routing(mainRequest.index().routing().substring(1));
                return;
            }
            switch (routingSpec) {
            case "keep":
                copyRouting(doc, index);
                break;
            case "discard":
                index.routing(null);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported routing command");
            }
        }

        private void copyRouting(SearchHit doc, IndexRequest index) {
            SearchHitField routing = doc.field("_routing");
            if (routing != null) {
                index.routing(routing.value());
            }
        }

        @Override
        protected IndexBySearchResponse buildResponse(long took) {
            return new IndexBySearchResponse(took, created(), updated(), batches(), versionConflicts(), failures());
        }
    }
}
