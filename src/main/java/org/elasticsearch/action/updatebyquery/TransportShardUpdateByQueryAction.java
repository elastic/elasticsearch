/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.TopLevelFixedBitSetCollector;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;

/**
 * Transport action that translates the shard update by query request into a bulk request. All actions are performed
 * locally and the bulk requests are then forwarded to the replica shards (this logic is done inside
 * {@link TransportShardBulkAction} which this transport action uses).
 */
public class TransportShardUpdateByQueryAction extends TransportAction<ShardUpdateByQueryRequest, ShardUpdateByQueryResponse> {

    public final static String ACTION_NAME = UpdateByQueryAction.NAME + "/shard";

    private static final Set<String> fields = new HashSet<String>();

    static {
        fields.add(RoutingFieldMapper.NAME);
        fields.add(ParentFieldMapper.NAME);
        fields.add(TTLFieldMapper.NAME);
        fields.add(SourceFieldMapper.NAME);
        fields.add(UidFieldMapper.NAME);
    }

    private final TransportShardBulkAction bulkAction;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final int batchSize;

    @Inject
    public TransportShardUpdateByQueryAction(Settings settings,
                                             ThreadPool threadPool,
                                             TransportShardBulkAction bulkAction,
                                             TransportService transportService,
                                             IndicesService indicesService,
                                             ClusterService clusterService,
                                             ScriptService scriptService) {
        super(settings, threadPool);
        this.bulkAction = bulkAction;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.batchSize = componentSettings.getAsInt("bulk_size", 1000);
        transportService.registerHandler(ACTION_NAME, new TransportHandler());
    }

    protected void doExecute(final ShardUpdateByQueryRequest request, final ActionListener<ShardUpdateByQueryResponse> listener) {
        String localNodeId = clusterService.state().nodes().localNodeId();
        if (!localNodeId.equals(request.targetNodeId())) {
            throw new ElasticSearchException("Request arrived on the wrong node. This shouldn't happen!");
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
        SearchContext searchContext = new SearchContext(
                0,
                shardSearchRequest,
                null, indexShard.searcher(), indexService, indexShard,
                scriptService
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
            throw new ElasticSearchException("Couldn't parse query from source.", e);
        }

        if (parsedQuery == null) {
            throw new ElasticSearchException("Query is required");
        }
        if (script == null) {
            throw new ElasticSearchException("Script is required");
        }
        context.parsedQuery(parsedQuery);
        ExecutableScript executableScript = scriptService.executable(scriptLang, script, params);
        return new UpdateByQueryContext(context, script, batchSize, clusterService.state(), executableScript);
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
                DocumentStoredFieldVisitor fieldVisitor = new DocumentStoredFieldVisitor(fields);
                indexReader.document(docID, fieldVisitor);
                Document document = fieldVisitor.getDocument();
                bulkItemRequests.add(new BulkItemRequest(counter, createRequest(request, document, indexReader)));

                if (++counter == batchSize) {
                    break;
                }
            }
        }

        // TODO: this is currently very similar to what we do in the update action, need to figure out how to nicely consolidate the two
        private ActionRequest createRequest(ShardUpdateByQueryRequest request, Document document, IndexReader indexReader) {
            Uid uid = Uid.createUid(document.get(UidFieldMapper.NAME));
            long version = -2;
            for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
                version = UidField.loadVersion(atomicReaderContext, new Term(UidFieldMapper.NAME, uid.toBytesRef()));
                if (version > 0) {
                    break;
                }
            }
            BytesReference _source = new BytesArray(document.getBinaryValue(SourceFieldMapper.NAME));
            String routing = document.get(RoutingFieldMapper.NAME);
            String parent = document.get(ParentFieldMapper.NAME);

            Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(_source, true);
            final XContentType updateSourceContentType = sourceAndContent.v1();

            updateByQueryContext.scriptContext.clear();
            updateByQueryContext.scriptContext.put("_source", sourceAndContent.v2());

            try {
                updateByQueryContext.executableScript.setNextVar("ctx", updateByQueryContext.scriptContext);
                updateByQueryContext.executableScript.run();
                // we need to unwrap the ctx...
                updateByQueryContext.scriptContext.putAll((Map<String, Object>) updateByQueryContext.executableScript.unwrap(updateByQueryContext.scriptContext));
            } catch (Exception e) {
                throw new ElasticSearchIllegalArgumentException("failed to execute script", e);
            }

            String operation = (String) updateByQueryContext.scriptContext.get("op");
            String timestamp = (String) updateByQueryContext.scriptContext.get("_timestamp");
            Object fetchedTTL = updateByQueryContext.scriptContext.get("_ttl");
            Long ttl = null;
            if (fetchedTTL != null) {
                if (fetchedTTL instanceof Number) {
                    ttl = ((Number) fetchedTTL).longValue();
                } else {
                    ttl = TimeValue.parseTimeValue((String) fetchedTTL, null).millis();
                }
            }

            Map<String, Object> updatedSourceAsMap = (Map<String, Object>) updateByQueryContext.scriptContext.get("_source");
            if (operation == null || "index".equals(operation)) {
                IndexRequest indexRequest = Requests.indexRequest(request.index()).type(uid.type()).id(uid.id())
                        .routing(routing)
                        .parent(parent)
                        .source(updatedSourceAsMap, updateSourceContentType)
                        .version(version)
                        .replicationType(request.replicationType())
                        .consistencyLevel(request.consistencyLevel())
                        .timestamp(timestamp)
                        .ttl(ttl);
                indexRequest.operationThreaded(false);

                MetaData metaData = updateByQueryContext.clusterState.metaData();
                MappingMetaData mappingMd = null;
                if (metaData.hasIndex(indexRequest.index())) {
                    mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
                }
                String aliasOrIndex = indexRequest.index();
                indexRequest.index(updateByQueryContext.clusterState.metaData().concreteIndex(indexRequest.index()));
                indexRequest.process(metaData, aliasOrIndex, mappingMd, false);
                return indexRequest;
            } else if ("delete".equals(operation)) {
                DeleteRequest deleteRequest = Requests.deleteRequest(request.index()).type(uid.type()).id(uid.id())
                        .routing(routing)
                        .parent(parent)
                        .version(version)
                        .replicationType(request.replicationType())
                        .consistencyLevel(request.consistencyLevel());
                deleteRequest.operationThreaded(false);
                return deleteRequest;
            } else {
                logger.warn("[{}][{}] used update operation [{}] for script [{}], doing nothing...", request.index(), request.shardId(), operation, updateByQueryContext.scriptString);
                return null;
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
    final String scriptString;
    final List<BulkItemRequest> bulkItemRequestsBulkList;
    final ClusterState clusterState;

    final Map<String, Object> scriptContext;
    final ExecutableScript executableScript;

    UpdateByQueryContext(SearchContext searchContext, String scriptString, int batchSize, ClusterState clusterState, ExecutableScript executableScript) {
        this.searchContext = searchContext;
        this.scriptString = scriptString;
        this.clusterState = clusterState;
        this.bulkItemRequestsBulkList = new ArrayList<BulkItemRequest>(batchSize);
        this.scriptContext = new HashMap<String, Object>();
        this.executableScript = executableScript;
    }
}