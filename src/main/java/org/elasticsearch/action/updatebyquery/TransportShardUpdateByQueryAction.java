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

package org.elasticsearch.action.updatebyquery;

import com.google.common.collect.Maps;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.TopLevelFixedBitSetCollector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fieldvisitor.JustUidFieldsVisitor;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport action that translates the shard update by query request into a bulk request. All actions are performed
 * locally and the bulk requests are then forwarded to the replica shards (this logic is done inside
 * {@link TransportShardBulkAction} which this transport action uses).
 */
public class TransportShardUpdateByQueryAction extends TransportAction<ShardUpdateByQueryRequest, ShardUpdateByQueryResponse> {

    public final static String ACTION_NAME = UpdateByQueryAction.NAME + "/shard";

    private final TransportShardBulkAction bulkAction;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final int batchSize;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;

    @Inject
    public TransportShardUpdateByQueryAction(Settings settings,
                                             ThreadPool threadPool,
                                             TransportShardBulkAction bulkAction,
                                             TransportService transportService,
                                             CacheRecycler cacheRecycler, IndicesService indicesService,
                                             ClusterService clusterService,
                                             ScriptService scriptService,
                                             PageCacheRecycler pageCacheRecycler,
                                             BigArrays bigArrays) {
        super(settings, threadPool);
        this.bulkAction = bulkAction;
        this.cacheRecycler = cacheRecycler;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.batchSize = componentSettings.getAsInt("bulk_size", 1000);
        transportService.registerHandler(ACTION_NAME, new TransportHandler());
    }

    protected void doExecute(final ShardUpdateByQueryRequest request, final ActionListener<ShardUpdateByQueryResponse> listener) {
        String localNodeId = clusterService.state().nodes().localNodeId();
        if (!localNodeId.equals(request.targetNodeId())) {
            throw new ElasticsearchException("Request arrived on the wrong node. This shouldn't happen!");
        }

        if (request.operationThreaded()) {
            request.beforeLocalFork();
            threadPool.executor(ThreadPool.Names.BULK).execute(new Runnable() {

                public void run() {
                    doExecuteInternal(request, listener);
                }

            });
        } else {
            doExecuteInternal(request, listener);
        }
    }

    private void doExecuteInternal(ShardUpdateByQueryRequest request, ActionListener<ShardUpdateByQueryResponse> listener) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest();
        shardSearchRequest.types(request.types());
        shardSearchRequest.filteringAliases(request.filteringAliases());
        SearchContext searchContext = new DefaultSearchContext(
                0,
                shardSearchRequest,
                null, indexShard.acquireSearcher("update_by_query"), indexService, indexShard,
                scriptService, cacheRecycler, pageCacheRecycler, bigArrays
        );
        SearchContext.setCurrent(searchContext);
        try {
            UpdateByQueryContext ubqContext = parseRequestSource(indexService, request, searchContext);
            searchContext.preProcess();
            // TODO: Work per segment. The collector should collect docs per segment instead of one big set of top level ids
            TopLevelFixedBitSetCollector bitSetCollector = new TopLevelFixedBitSetCollector(searchContext.searcher().getIndexReader().maxDoc());
            searchContext.searcher().search(searchContext.query(), searchContext.aliasFilter(), bitSetCollector);
            FixedBitSet docsToUpdate = bitSetCollector.getBitSet();

            int docsToUpdateCount = docsToUpdate.cardinality();
            logger.trace("[{}][{}] {} docs to update", request.index(), request.shardId(), docsToUpdateCount);

            if (docsToUpdateCount == 0) {
                ShardUpdateByQueryResponse response = new ShardUpdateByQueryResponse(request.shardId());
                listener.onResponse(response);
                searchContext.release();
                return;
            }
            BatchedShardUpdateByQueryExecutor bulkExecutor = new BatchedShardUpdateByQueryExecutor(
                    listener, docsToUpdate, request, ubqContext
            );
            bulkExecutor.executeBulkIndex();
        } catch (Throwable t) {
            // If we end up here then BatchedShardUpdateByQueryExecutor#finalizeBulkActions isn't invoked
            // so we need to release the search context.
            searchContext.release();
            listener.onFailure(t);
        } finally {
            SearchContext.removeCurrent();
        }
    }

    private UpdateByQueryContext parseRequestSource(IndexService indexService, ShardUpdateByQueryRequest request, SearchContext context) {
        ParsedQuery parsedQuery = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> params = Maps.newHashMap();
        try {
            XContentParser parser = XContentHelper.createParser(request.source());
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("query".equals(fieldName)) {
                        parsedQuery = indexService.queryParserService().parse(parser);
                    } else if ("query_binary".equals(fieldName)) {
                        parser.nextToken();
                        byte[] querySource = parser.binaryValue();
                        XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource);
                        parsedQuery = indexService.queryParserService().parse(qSourceParser);
                    } else if ("script".equals(fieldName)) {
                        parser.nextToken();
                        script = parser.text();
                    } else if ("lang".equals(fieldName)) {
                        parser.nextToken();
                        scriptLang = parser.text();
                    } else if ("params".equals(fieldName)) {
                        parser.nextToken();
                        params = parser.map();
                    }
                }
            }
        } catch (Exception e) {
            throw new ElasticsearchException("Couldn't parse query from source.", e);
        }

        if (parsedQuery == null) {
            throw new ElasticsearchException("Query is required");
        }
        if (script == null) {
            throw new ElasticsearchException("Script is required");
        }
        context.parsedQuery(parsedQuery);
        return new UpdateByQueryContext(context, batchSize, clusterService.state(), script, scriptLang, params);
    }


    class BatchedShardUpdateByQueryExecutor implements ActionListener<BulkShardResponse> {

        private final ActionListener<ShardUpdateByQueryResponse> finalResponseListener;
        private final DocIdSetIterator iterator;
        private final int matches;
        private final ShardUpdateByQueryRequest request;
        private final List<BulkItemResponse> receivedBulkItemResponses;
        private final UpdateByQueryContext updateByQueryContext;

        // Counter for keeping tracker number of docs that have been updated.
        // No need for sync now since onResponse method in synchronized
        private int updated;

        BatchedShardUpdateByQueryExecutor(ActionListener<ShardUpdateByQueryResponse> finalResponseListener,
                                          FixedBitSet docsToUpdate,
                                          ShardUpdateByQueryRequest request,
                                          UpdateByQueryContext updateByQueryContext) {
            this.iterator = docsToUpdate.iterator();
            this.matches = docsToUpdate.cardinality();
            this.request = request;
            this.finalResponseListener = finalResponseListener;
            this.receivedBulkItemResponses = new ArrayList<BulkItemResponse>();
            this.updateByQueryContext = updateByQueryContext;
        }

        // Call can be invoked with a Network thread. Replica isn't on the same node... Therefore when
        // need to continue with the bulk do it in a new thread. One thread will enter at the time.
        public synchronized void onResponse(BulkShardResponse bulkShardResponse) {
            try {
                for (BulkItemResponse itemResponse : bulkShardResponse.getResponses()) {
                    if (!itemResponse.isFailed()) {
                        updated++;
                    }
                    switch (request.bulkResponseOptions()) {
                        case ALL:
                            receivedBulkItemResponses.add(itemResponse);
                            break;
                        case FAILED:
                            if (itemResponse.isFailed()) {
                                receivedBulkItemResponses.add(itemResponse);
                            }
                            break;
                        case NONE:
                            break;
                    }
                }
                if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                    finalizeBulkActions(null);
                } else {
                    threadPool.executor(ThreadPool.Names.BULK).execute(new Runnable() {
                        public void run() {
                            try {
                                executeBulkIndex();
                            } catch (Throwable e) {
                                onFailure(e);
                            }
                        }
                    });
                }
            } catch (Throwable t) {
                onFailure(t);
            }
        }

        public synchronized void onFailure(Throwable e) {
            try {
                logger.debug("error while executing bulk operations for an update by query action, sending partial response...", e);
                finalizeBulkActions(e);
            } catch (Throwable t) {
                finalResponseListener.onFailure(t);
            }
        }

        public void executeBulkIndex() throws IOException {
            fillBatch(iterator, updateByQueryContext.searchContext.searcher().getIndexReader(), request, updateByQueryContext.bulkItemRequestsBulkList);
            logger.trace("[{}][{}] executing bulk request with size {}", request.index(), request.shardId(), updateByQueryContext.bulkItemRequestsBulkList.size());
            if (updateByQueryContext.bulkItemRequestsBulkList.isEmpty()) {
                onResponse(new BulkShardResponse(new ShardId(request.index(), request.shardId()), new BulkItemResponse[0]));
            } else {
                // We are already on the primary shard. Only have network traffic for replica shards
                // Also no need for threadpool b/c TransUpdateAction uses it already for local requests.
                BulkItemRequest[] bulkItemRequests =
                        updateByQueryContext.bulkItemRequestsBulkList.toArray(new BulkItemRequest[updateByQueryContext.bulkItemRequestsBulkList.size()]);
                // We clear the list, since the array is already created
                updateByQueryContext.bulkItemRequestsBulkList.clear();
                final BulkShardRequest bulkShardRequest = new BulkShardRequest(
                        request.index(), request.shardId(), false, bulkItemRequests
                );
                // The batches are already threaded... No need for new thread
                bulkShardRequest.operationThreaded(false);
                bulkAction.execute(bulkShardRequest, this);
            }
        }

        private void finalizeBulkActions(Throwable e) {
            updateByQueryContext.searchContext.release();
            BulkItemResponse[] bulkResponses = receivedBulkItemResponses.toArray(new BulkItemResponse[receivedBulkItemResponses.size()]);
            receivedBulkItemResponses.clear();
            ShardUpdateByQueryResponse finalResponse = new ShardUpdateByQueryResponse(
                    request.shardId(), matches, updated, bulkResponses
            );

            if (e != null) {
                finalResponse.failedShardExceptionMessage(ExceptionsHelper.detailedMessage(e));
            }
            finalResponseListener.onResponse(finalResponse);
        }

        // TODO: Work per segment. The collector should collect docs per segment instead of one big set of top level ids
        private void fillBatch(DocIdSetIterator iterator, IndexReader indexReader, ShardUpdateByQueryRequest request,
                               List<BulkItemRequest> bulkItemRequests) throws IOException {
            int counter = 0;
            for (int docID = iterator.nextDoc(); docID != DocIdSetIterator.NO_MORE_DOCS; docID = iterator.nextDoc()) {
                JustUidFieldsVisitor fieldVisitor = new JustUidFieldsVisitor();
                indexReader.document(docID, fieldVisitor);
                Uid uid = fieldVisitor.uid();
                UpdateRequest updateRequest = new UpdateRequest(request.index(), uid.type(), uid.id())
                        .scriptLang(updateByQueryContext.scriptLang)
                        .scriptParams(updateByQueryContext.scriptParams)
                        .script(updateByQueryContext.scriptString);

                bulkItemRequests.add(new BulkItemRequest(counter, updateRequest));

                if (++counter == batchSize) {
                    break;
                }
            }
        }

    }

    class TransportHandler extends BaseTransportRequestHandler<ShardUpdateByQueryRequest> {

        public ShardUpdateByQueryRequest newInstance() {
            return new ShardUpdateByQueryRequest();
        }

        public String executor() {
            return ThreadPool.Names.SAME;
        }

        public void messageReceived(final ShardUpdateByQueryRequest request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<ShardUpdateByQueryResponse>() {

                public void onResponse(ShardUpdateByQueryResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }

            });
        }
    }

}

class UpdateByQueryContext {

    final SearchContext searchContext;
    final List<BulkItemRequest> bulkItemRequestsBulkList;
    final ClusterState clusterState;

    final String scriptString;
    final String scriptLang;
    final Map<String, Object> scriptParams;

    UpdateByQueryContext(SearchContext searchContext, int batchSize, ClusterState clusterState, String scriptString, String scriptLang, Map<String, Object> scriptParams) {
        this.searchContext = searchContext;
        this.clusterState = clusterState;
        this.bulkItemRequestsBulkList = new ArrayList<BulkItemRequest>(batchSize);
        this.scriptString = scriptString;
        this.scriptLang = scriptLang;
        this.scriptParams = scriptParams;
    }
}