/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.EXCLUDED_DATA_STREAMS_KEY;

/**
 * retries on retryable cluster blocks, resolves item requests,
 * constructs shard bulk requests and delegates execution to shard bulk action
 */
final class BulkOperation extends ActionRunnable<BulkResponse> {

    private static final Logger logger = LogManager.getLogger(BulkOperation.class);

    private final Task task;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private BulkRequest bulkRequest; // set to null once all requests are sent out
    private final ActionListener<BulkResponse> listener;
    private final AtomicArray<BulkItemResponse> responses;
    private final long startTimeNanos;
    private final ClusterStateObserver observer;
    private final Map<String, IndexNotFoundException> indicesThatCannotBeCreated;
    private final String executorName;
    private final LongSupplier relativeTimeProvider;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private NodeClient client;

    BulkOperation(
        Task task,
        ThreadPool threadPool,
        String executorName,
        ClusterService clusterService,
        BulkRequest bulkRequest,
        NodeClient client,
        AtomicArray<BulkItemResponse> responses,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LongSupplier relativeTimeProvider,
        long startTimeNanos,
        ActionListener<BulkResponse> listener
    ) {
        super(listener);
        this.task = task;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.responses = responses;
        this.bulkRequest = bulkRequest;
        this.listener = listener;
        this.startTimeNanos = startTimeNanos;
        this.indicesThatCannotBeCreated = indicesThatCannotBeCreated;
        this.executorName = executorName;
        this.relativeTimeProvider = relativeTimeProvider;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.observer = new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext());
    }

    @Override
    protected void doRun() {
        assert bulkRequest != null;
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (handleBlockExceptions(clusterState)) {
            return;
        }
        Map<ShardId, List<BulkItemRequest>> requestsByShard = groupRequestsByShards(clusterState);
        executeBulkRequestsByShard(requestsByShard, clusterState);
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeNanos);
    }

    private Map<ShardId, List<BulkItemRequest>> groupRequestsByShards(ClusterState clusterState) {
        final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
        Metadata metadata = clusterState.metadata();
        // Group the requests by ShardId -> Operations mapping
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();

        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docWriteRequest = bulkRequest.requests.get(i);
            // the request can only be null because we set it to null in the previous step, so it gets ignored
            if (docWriteRequest == null) {
                continue;
            }
            if (addFailureIfRequiresAliasAndAliasIsMissing(docWriteRequest, i, metadata)) {
                continue;
            }
            if (addFailureIfIndexCannotBeCreated(docWriteRequest, i)) {
                continue;
            }
            if (addFailureIfRequiresDataStreamAndNoParentDataStream(docWriteRequest, i, metadata)) {
                continue;
            }
            IndexAbstraction ia = null;
            boolean includeDataStreams = docWriteRequest.opType() == DocWriteRequest.OpType.CREATE;
            try {
                ia = concreteIndices.resolveIfAbsent(docWriteRequest);
                if (ia.isDataStreamRelated() && includeDataStreams == false) {
                    throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                }
                // The ConcreteIndices#resolveIfAbsent(...) method validates via IndexNameExpressionResolver whether
                // an operation is allowed in index into a data stream, but this isn't done when resolve call is cached, so
                // the validation needs to be performed here too.
                if (ia.getParentDataStream() != null &&
                // avoid valid cases when directly indexing into a backing index
                // (for example when directly indexing into .ds-logs-foobar-000001)
                    ia.getName().equals(docWriteRequest.index()) == false && docWriteRequest.opType() != DocWriteRequest.OpType.CREATE) {
                    throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                }

                TransportBulkAction.prohibitCustomRoutingOnDataStream(docWriteRequest, metadata);
                TransportBulkAction.prohibitAppendWritesInBackingIndices(docWriteRequest, metadata);
                docWriteRequest.routing(metadata.resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));

                final Index concreteIndex = docWriteRequest.getConcreteWriteIndex(ia, metadata);
                if (addFailureIfIndexIsClosed(docWriteRequest, concreteIndex, i, metadata)) {
                    continue;
                }
                IndexRouting indexRouting = concreteIndices.routing(concreteIndex);
                docWriteRequest.process(indexRouting);
                int shardId = docWriteRequest.route(indexRouting);
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(
                    new ShardId(concreteIndex, shardId),
                    shard -> new ArrayList<>()
                );
                shardRequests.add(new BulkItemRequest(i, docWriteRequest));
            } catch (ElasticsearchParseException | IllegalArgumentException | RoutingMissingException | ResourceNotFoundException e) {
                String name = ia != null ? ia.getName() : docWriteRequest.index();
                BulkItemResponse.Failure failure = new BulkItemResponse.Failure(name, docWriteRequest.id(), e);
                BulkItemResponse bulkItemResponse = BulkItemResponse.failure(i, docWriteRequest.opType(), failure);
                responses.set(i, bulkItemResponse);
                // make sure the request gets never processed again
                bulkRequest.requests.set(i, null);
            }
        }
        return requestsByShard;
    }

    private void executeBulkRequestsByShard(Map<ShardId, List<BulkItemRequest>> requestsByShard, ClusterState clusterState) {
        if (requestsByShard.isEmpty()) {
            listener.onResponse(
                new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos))
            );
            return;
        }

        String nodeId = clusterService.localNode().getId();
        Runnable onBulkItemsComplete = () -> {
            listener.onResponse(
                new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos))
            );
            // Allow memory for bulk shard request items to be reclaimed before all items have been completed
            bulkRequest = null;
        };

        try (RefCountingRunnable bulkItemRequestCompleteRefCount = new RefCountingRunnable(onBulkItemsComplete)) {
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();

                BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    shardId,
                    bulkRequest.getRefreshPolicy(),
                    requests.toArray(new BulkItemRequest[0])
                );
                var indexMetadata = clusterState.getMetadata().index(shardId.getIndexName());
                if (indexMetadata != null && indexMetadata.getFieldsForModels().isEmpty() == false) {
                    bulkShardRequest.setFieldInferenceMetadata(indexMetadata.getFieldsForModels());
                }
                bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(bulkRequest.timeout());
                bulkShardRequest.routedBasedOnClusterVersion(clusterState.version());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }
                executeBulkShardRequest(bulkShardRequest, bulkItemRequestCompleteRefCount.acquire());
            }
        }
    }

    private void executeBulkShardRequest(BulkShardRequest bulkShardRequest, Releasable releaseOnFinish) {
        client.executeLocally(TransportShardBulkAction.TYPE, bulkShardRequest, new ActionListener<>() {
            @Override
            public void onResponse(BulkShardResponse bulkShardResponse) {
                for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                    // we may have no response if item failed
                    if (bulkItemResponse.getResponse() != null) {
                        bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                    }
                    responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                }
                releaseOnFinish.close();
            }

            @Override
            public void onFailure(Exception e) {
                // create failures for all relevant requests
                for (BulkItemRequest request : bulkShardRequest.items()) {
                    final String indexName = request.index();
                    DocWriteRequest<?> docWriteRequest = request.request();
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexName, docWriteRequest.id(), e);
                    responses.set(request.id(), BulkItemResponse.failure(request.id(), docWriteRequest.opType(), failure));
                }
                releaseOnFinish.close();
            }
        });
    }

    private boolean handleBlockExceptions(ClusterState state) {
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
        if (blockException != null) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked, scheduling a retry", blockException);
                retry(blockException);
            } else {
                onFailure(blockException);
            }
            return true;
        }
        return false;
    }

    void retry(Exception failure) {
        assert failure != null;
        if (observer.isTimedOut()) {
            // we running as a last attempt after a timeout has happened. don't retry
            onFailure(failure);
            return;
        }
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                /*
                 * This is called on the cluster state update thread pool
                 * but we'd prefer to coordinate the bulk request on the
                 * write thread pool just to make sure the cluster state
                 * update thread doesn't get clogged up.
                 */
                dispatchRetry();
            }

            @Override
            public void onClusterServiceClose() {
                onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                /*
                 * Try one more time.... This is called on the generic
                 * thread pool but out of an abundance of caution we
                 * switch over to the write thread pool that we expect
                 * to coordinate the bulk request.
                 */
                dispatchRetry();
            }

            private void dispatchRetry() {
                threadPool.executor(executorName).submit(BulkOperation.this);
            }
        });
    }

    private boolean addFailureIfRequiresAliasAndAliasIsMissing(DocWriteRequest<?> request, int idx, final Metadata metadata) {
        if (request.isRequireAlias() && (metadata.hasAlias(request.index()) == false)) {
            Exception exception = new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + request.index() + "] is not an alias",
                request.index()
            );
            addFailure(request, idx, exception);
            return true;
        }
        return false;
    }

    private boolean addFailureIfRequiresDataStreamAndNoParentDataStream(DocWriteRequest<?> request, int idx, final Metadata metadata) {
        if (request.isRequireDataStream() && (metadata.indexIsADataStream(request.index()) == false)) {
            Exception exception = new ResourceNotFoundException(
                "[" + DocWriteRequest.REQUIRE_DATA_STREAM + "] request flag is [true] and [" + request.index() + "] is not a data stream",
                request.index()
            );
            addFailure(request, idx, exception);
            return true;
        }
        return false;
    }

    private boolean addFailureIfIndexIsClosed(DocWriteRequest<?> request, Index concreteIndex, int idx, final Metadata metadata) {
        IndexMetadata indexMetadata = metadata.getIndexSafe(concreteIndex);
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            addFailure(request, idx, new IndexClosedException(concreteIndex));
            return true;
        }
        return false;
    }

    private boolean addFailureIfIndexCannotBeCreated(DocWriteRequest<?> request, int idx) {
        IndexNotFoundException cannotCreate = indicesThatCannotBeCreated.get(request.index());
        if (cannotCreate != null) {
            addFailure(request, idx, cannotCreate);
            return true;
        }
        return false;
    }

    private void addFailure(DocWriteRequest<?> request, int idx, Exception unavailableException) {
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.id(), unavailableException);
        BulkItemResponse bulkItemResponse = BulkItemResponse.failure(idx, request.opType(), failure);
        responses.set(idx, bulkItemResponse);
        // make sure the request gets never processed again
        bulkRequest.requests.set(idx, null);
    }

    private static class ConcreteIndices {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, IndexAbstraction> indexAbstractions = new HashMap<>();
        private final Map<Index, IndexRouting> routings = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        IndexAbstraction resolveIfAbsent(DocWriteRequest<?> request) {
            try {
                IndexAbstraction indexAbstraction = indexAbstractions.get(request.index());
                if (indexAbstraction == null) {
                    indexAbstraction = indexNameExpressionResolver.resolveWriteIndexAbstraction(state, request);
                    indexAbstractions.put(request.index(), indexAbstraction);
                }
                return indexAbstraction;
            } catch (IndexNotFoundException e) {
                if (e.getMetadataKeys().contains(EXCLUDED_DATA_STREAMS_KEY)) {
                    throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams", e);
                } else {
                    throw e;
                }
            }
        }

        IndexRouting routing(Index index) {
            IndexRouting routing = routings.get(index);
            if (routing == null) {
                routing = IndexRouting.fromIndexMetadata(state.metadata().getIndexSafe(index));
                routings.put(index, routing);
            }
            return routing;
        }
    }
}
