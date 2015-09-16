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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Delete-By-Query implementation that uses efficient scrolling and bulks deletions to delete large set of documents.
 */
public class TransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, DeleteByQueryResponse> {

    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final Client client;

    @Inject
    public TransportDeleteByQueryAction(Settings settings, ThreadPool threadPool, Client client,
                                           TransportSearchAction transportSearchAction,
                                           TransportSearchScrollAction transportSearchScrollAction,
                                           TransportService transportService, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteByQueryAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, DeleteByQueryRequest::new);
        this.searchAction = transportSearchAction;
        this.scrollAction = transportSearchScrollAction;
        this.client = client;
    }

    @Override
    protected void doExecute(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        new AsyncDeleteByQueryAction(request, listener).start();
    }

    class AsyncDeleteByQueryAction {

        private final DeleteByQueryRequest request;
        private final ActionListener<DeleteByQueryResponse> listener;

        private final long startTime;
        private final AtomicBoolean timedOut;
        private final AtomicLong total;

        private volatile ShardOperationFailedException[] shardFailures;
        private final Map<String, IndexDeleteByQueryResponse> results;

        AsyncDeleteByQueryAction(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.startTime = threadPool.estimatedTimeInMillis();
            this.timedOut = new AtomicBoolean(false);
            this.total = new AtomicLong(0L);
            this.shardFailures = ShardSearchFailure.EMPTY_ARRAY;
            this.results = new HashMap<>();
        }

        public void start() {
            executeScan();
        }

        void executeScan() {
            try {
                final SearchRequest scanRequest = new SearchRequest(request.indices()).types(request.types()).indicesOptions(request.indicesOptions());
                scanRequest.scroll(request.scroll());
                if (request.routing() != null) {
                    scanRequest.routing(request.routing());
                }

                SearchSourceBuilder source = new SearchSourceBuilder()
                        .query(request.source())
                        .fields("_routing", "_parent")
                        .sort("_doc") // important for performance
                        .fetchSource(false)
                        .version(true);
                if (request.size() > 0) {
                    source.size(request.size());
                }
                if (request.timeout() != null) {
                    source.timeout(request.timeout());
                }
                scanRequest.source(source);

                logger.trace("executing scan request");
                searchAction.execute(scanRequest, new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        long hits = searchResponse.getHits().getTotalHits();
                        logger.trace("first request executed: found [{}] document(s) to delete", hits);
                        total.set(hits);
                        deleteHits(null, searchResponse);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
            } catch (Throwable t) {
                logger.error("unable to execute the initial scan request of delete by query", t);
                listener.onFailure(t);
            }
        }

        void executeScroll(final String scrollId) {
            try {
                logger.trace("executing scroll request [{}]", scrollId);
                scrollAction.execute(new SearchScrollRequest(scrollId).scroll(request.scroll()), new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse scrollResponse) {
                        deleteHits(scrollId, scrollResponse);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("scroll request [{}] failed, scrolling document(s) is stopped", e, scrollId);
                        finishHim(scrollId, hasTimedOut(), e);
                    }
                });
            } catch (Throwable t) {
                logger.error("unable to execute scroll request [{}]", t, scrollId);
                finishHim(scrollId, false, t);
            }
        }

        void deleteHits(String scrollId, SearchResponse scrollResponse) {
            final SearchHit[] docs = scrollResponse.getHits().getHits();
            final String nextScrollId = scrollResponse.getScrollId();
            addShardFailures(scrollResponse.getShardFailures());

            if (logger.isTraceEnabled()) {
                logger.trace("scroll request [{}] executed: [{}] document(s) returned", scrollId, docs.length);
            }

            if ((docs.length == 0) || (nextScrollId == null)) {
                logger.trace("scrolling documents terminated");
                // if scrollId is null we are on the first request - just pass the nextScrollId which sill be non-null if the query matched no docs
                finishHim(scrollId == null ? nextScrollId : scrollId, false, null);
                return;
            }

            if (hasTimedOut()) {
                logger.trace("scrolling documents timed out");
                // if scrollId is null we are on the first request - just pass the nextScrollId which sill be non-null if the query matched no docs
                finishHim(scrollId == null ? nextScrollId : scrollId, true, null);
                return;
            }

            // Delete the scrolled documents using the Bulk API
            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit doc : docs) {
                DeleteRequest delete = new DeleteRequest(doc.index(), doc.type(), doc.id()).version(doc.version());
                SearchHitField routing = doc.field("_routing");
                if (routing != null) {
                    delete.routing((String) routing.value());
                }
                SearchHitField parent = doc.field("_parent");
                if (parent != null) {
                    delete.parent((String) parent.value());
                }
                bulkRequest.add(delete);
            }

            logger.trace("executing bulk request with [{}] deletions", bulkRequest.numberOfActions());
            client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    onBulkResponse(nextScrollId, bulkResponse);
                }

                @Override
                public void onFailure(Throwable e) {
                    onBulkFailure(nextScrollId, docs, e);
                }
            });
        }

        void onBulkResponse(String scrollId, BulkResponse bulkResponse) {
            try {
                for (BulkItemResponse item : bulkResponse.getItems()) {
                    IndexDeleteByQueryResponse indexCounter = results.get(item.getIndex());
                    if (indexCounter == null) {
                        indexCounter = new IndexDeleteByQueryResponse(item.getIndex());
                    }
                    indexCounter.incrementFound();
                    if (item.isFailed()) {
                        indexCounter.incrementFailed();
                    } else {
                        DeleteResponse delete = item.getResponse();
                        if (delete.isFound()) {
                            indexCounter.incrementDeleted();
                        } else {
                            indexCounter.incrementMissing();
                        }
                    }
                    results.put(item.getIndex(), indexCounter);
                }

                logger.trace("scrolling next batch of document(s) with scroll id [{}]", scrollId);
                executeScroll(scrollId);
            } catch (Throwable t) {
                logger.error("unable to process bulk response", t);
                finishHim(scrollId, false, t);
            }
        }

        void onBulkFailure(String scrollId, SearchHit[] docs, Throwable failure) {
            try {
                logger.trace("execution of scroll request failed: {}", failure.getMessage());
                for (SearchHit doc : docs) {
                    IndexDeleteByQueryResponse indexCounter = results.get(doc.index());
                    if (indexCounter == null) {
                        indexCounter = new IndexDeleteByQueryResponse(doc.index());
                    }
                    indexCounter.incrementFound();
                    indexCounter.incrementFailed();
                    results.put(doc.getIndex(), indexCounter);
                }

                logger.trace("scrolling document terminated due to scroll request failure [{}]", scrollId);
                finishHim(scrollId, hasTimedOut(), failure);
            } catch (Throwable t) {
                logger.error("unable to process bulk failure", t);
                finishHim(scrollId, false, t);
            }
        }

        void finishHim(final String scrollId, boolean scrollTimedOut, Throwable failure) {
            try {
                if (scrollTimedOut) {
                    logger.trace("delete-by-query response marked as timed out");
                    timedOut.set(true);
                }

                if (Strings.hasText(scrollId)) {
                    client.prepareClearScroll().addScrollId(scrollId).execute(new ActionListener<ClearScrollResponse>() {
                        @Override
                        public void onResponse(ClearScrollResponse clearScrollResponse) {
                            logger.trace("scroll id [{}] cleared", scrollId);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.warn("unable to clear scroll id [{}]: {}", scrollId, e.getMessage());
                        }
                    });
                }

                if (failure != null) {
                    logger.trace("scrolling document(s) terminated with failures: {}", failure.getMessage());
                    listener.onFailure(failure);
                } else {
                    logger.trace("scrolling document(s) terminated with success");
                    listener.onResponse(buildResponse());
                }
            } catch (Throwable t) {
                listener.onFailure(t);
            }
        }

        boolean hasTimedOut() {
            return request.timeout() != null && (threadPool.estimatedTimeInMillis() >= (startTime + request.timeout().millis()));
        }

        void addShardFailure(ShardOperationFailedException failure) {
            addShardFailures(new ShardOperationFailedException[]{failure});
        }

        void addShardFailures(ShardOperationFailedException[] failures) {
            if (!CollectionUtils.isEmpty(failures)) {
                ShardOperationFailedException[] duplicates = new ShardOperationFailedException[shardFailures.length + failures.length];
                System.arraycopy(shardFailures, 0, duplicates, 0, shardFailures.length);
                System.arraycopy(failures, 0, duplicates, shardFailures.length, failures.length);
                shardFailures = ExceptionsHelper.groupBy(duplicates);
            }
        }

        protected DeleteByQueryResponse buildResponse() {
            long took = threadPool.estimatedTimeInMillis() - startTime;
            long deleted = 0;
            long missing = 0;
            long failed = 0;

            // Calculates the total number  deleted/failed/missing documents
            for (IndexDeleteByQueryResponse result : results.values()) {
                deleted = deleted + result.getDeleted();
                missing = missing + result.getMissing();
                failed = failed + result.getFailed();
            }
            IndexDeleteByQueryResponse[] indices = results.values().toArray(new IndexDeleteByQueryResponse[results.size()]);
            return new DeleteByQueryResponse(took, timedOut.get(), total.get(), deleted, missing, failed, indices, shardFailures);
        }
    }
}
