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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    private BulkRequest bulkRequest; // set to null once all requests are completed
    private final ActionListener<BulkResponse> listener;
    private final AtomicArray<BulkItemResponse> responses;
    private final ConcurrentLinkedQueue<BulkItemRequest> failureStoreRedirects = new ConcurrentLinkedQueue<>();
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
        this(
            task,
            threadPool,
            executorName,
            clusterService,
            bulkRequest,
            client,
            responses,
            indicesThatCannotBeCreated,
            indexNameExpressionResolver,
            relativeTimeProvider,
            startTimeNanos,
            listener,
            new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext())
        );
    }

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
        ActionListener<BulkResponse> listener,
        ClusterStateObserver observer
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
        this.observer = observer;
    }

    @Override
    protected void doRun() {
        assert bulkRequest != null;
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (handleBlockExceptions(clusterState, BulkOperation.this, this::onFailure)) {
            return;
        }
        Map<ShardId, List<BulkItemRequest>> requestsByShard = groupBulkRequestsByShards(clusterState);
        executeBulkRequestsByShard(requestsByShard, clusterState, this::redirectFailuresOrCompleteBulkOperation);
    }

    private void doRedirectFailures() {
        assert failureStoreRedirects.isEmpty() != true : "Attempting to redirect failures, but none were present in the queue";
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (handleBlockExceptions(clusterState, ActionRunnable.run(listener, this::doRedirectFailures), this::discardRedirectsAndFinish)) {
            return;
        }
        Map<ShardId, List<BulkItemRequest>> requestsByShard = groupRedirectsByShards(clusterState);
        executeBulkRequestsByShard(requestsByShard, clusterState, this::completeBulkOperation);
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeNanos);
    }

    private Map<ShardId, List<BulkItemRequest>> groupBulkRequestsByShards(ClusterState clusterState) {
        return groupRequestsByShards(
            clusterState,
            Iterators.zip(bulkRequest.requests.iterator(), BulkItemRequest::new),
            BulkOperation::validateWriteIndex
        );
    }

    private Map<ShardId, List<BulkItemRequest>> groupRedirectsByShards(ClusterState clusterState) {
        return groupRequestsByShards(
            clusterState,
            Iterators.fromSupplier(failureStoreRedirects::poll),
            (ia, ignore) -> validateRedirectIndex(ia)
        );
    }

    private Map<ShardId, List<BulkItemRequest>> groupRequestsByShards(
        ClusterState clusterState,
        Iterator<BulkItemRequest> it,
        BiConsumer<IndexAbstraction, DocWriteRequest<?>> indexOperationValidator
    ) {
        final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
        Metadata metadata = clusterState.metadata();
        // Group the requests by ShardId -> Operations mapping
        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();

        while (it.hasNext()) {
            BulkItemRequest bulkItemRequest = it.next();
            DocWriteRequest<?> docWriteRequest = bulkItemRequest.request();

            // the request can only be null because we set it to null in the previous step, so it gets ignored
            if (docWriteRequest == null) {
                continue;
            }
            if (addFailureIfRequiresAliasAndAliasIsMissing(docWriteRequest, bulkItemRequest.id(), metadata)) {
                continue;
            }
            if (addFailureIfIndexCannotBeCreated(docWriteRequest, bulkItemRequest.id())) {
                continue;
            }
            if (addFailureIfRequiresDataStreamAndNoParentDataStream(docWriteRequest, bulkItemRequest.id(), metadata)) {
                continue;
            }
            IndexAbstraction ia = null;
            try {
                ia = concreteIndices.resolveIfAbsent(docWriteRequest);
                indexOperationValidator.accept(ia, docWriteRequest);

                TransportBulkAction.prohibitCustomRoutingOnDataStream(docWriteRequest, metadata);
                TransportBulkAction.prohibitAppendWritesInBackingIndices(docWriteRequest, metadata);
                docWriteRequest.routing(metadata.resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));

                final Index concreteIndex = docWriteRequest.getConcreteWriteIndex(ia, metadata);
                if (addFailureIfIndexIsClosed(docWriteRequest, concreteIndex, bulkItemRequest.id(), metadata)) {
                    continue;
                }
                IndexRouting indexRouting = concreteIndices.routing(concreteIndex);
                docWriteRequest.process(indexRouting);
                int shardId = docWriteRequest.route(indexRouting);
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(
                    new ShardId(concreteIndex, shardId),
                    shard -> new ArrayList<>()
                );
                shardRequests.add(bulkItemRequest);
            } catch (ElasticsearchParseException | IllegalArgumentException | RoutingMissingException | ResourceNotFoundException e) {
                String name = ia != null ? ia.getName() : docWriteRequest.index();
                addFailure(docWriteRequest, bulkItemRequest.id(), name, e);
            }
        }
        return requestsByShard;
    }

    /**
     * Validates that an index abstraction is capable of receiving the provided write request
     */
    private static void validateWriteIndex(IndexAbstraction ia, DocWriteRequest<?> docWriteRequest) {
        boolean includeDataStreams = docWriteRequest.opType() == DocWriteRequest.OpType.CREATE;
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
    }

    /**
     * Validates that an index abstraction is capable of receiving a failure store redirect
     */
    private static void validateRedirectIndex(IndexAbstraction ia) {
        if (ia.isDataStreamRelated() == false) {
            // We should only be dealing with traffic targeting concrete data streams.
            throw new IllegalArgumentException(
                "only write ops to data streams with enabled failure stores can be redirected on failure."
            );
        }
    }

    private void executeBulkRequestsByShard(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        ClusterState clusterState,
        Runnable onRequestsCompleted
    ) {
        if (requestsByShard.isEmpty()) {
            onRequestsCompleted.run();
            return;
        }

        String nodeId = clusterService.localNode().getId();
        try (RefCountingRunnable bulkItemRequestCompleteRefCount = new RefCountingRunnable(onRequestsCompleted)) {
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();

                BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    shardId,
                    bulkRequest.getRefreshPolicy(),
                    requests.toArray(new BulkItemRequest[0])
                );
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

    private void redirectFailuresOrCompleteBulkOperation() {
        if (failureStoreRedirects.isEmpty() == false) {
            doRedirectFailures();
        } else {
            completeBulkOperation();
        }
    }

    private void completeBulkOperation() {
        listener.onResponse(
            new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos))
        );
        // Allow memory for bulk shard request items to be reclaimed before all items have been completed
        bulkRequest = null;
    }


    /**
     * Discards all failure store redirections and completes the bulk request.
     * @param exception any documents that could have been redirected will have this exception added as a suppressed exception
     *                  on their original failure information.
     */
    private void discardRedirectsAndFinish(Exception exception) {
        assert failureStoreRedirects.isEmpty() != true : "Attempting to discard redirects, but there were none to discard";
        Iterator<BulkItemRequest> redirectedBulkItemIterator = Iterators.fromSupplier(failureStoreRedirects::poll);
        while (redirectedBulkItemIterator.hasNext()) {
            BulkItemRequest cancelledRedirectBulkItem = redirectedBulkItemIterator.next();
            int slot = cancelledRedirectBulkItem.id();
            BulkItemResponse originalFailure = responses.get(slot);
            if (originalFailure.isFailed()) {
                originalFailure.getFailure().getCause().addSuppressed(exception);
            }
        }
        completeBulkOperation();
    }

    private void executeBulkShardRequest(BulkShardRequest bulkShardRequest, Releasable releaseOnFinish) {
        client.executeLocally(TransportShardBulkAction.TYPE, bulkShardRequest, new ActionListener<>() {

            // Lazily get the cluster state to avoid keeping it around longer than it is needed
            private ClusterState clusterState = null;
            private ClusterState getClusterState() {
                if (clusterState == null) {
                    clusterState = clusterService.state();
                }
                return clusterState;
            }

            @Override
            public void onResponse(BulkShardResponse bulkShardResponse) {
                for (int idx = 0; idx < bulkShardResponse.getResponses().length; idx++) {
                    // We zip the requests and responses together so that we can identify failed documents and potentially store them
                    BulkItemRequest bulkItemRequest = bulkShardRequest.items()[idx];
                    BulkItemResponse bulkItemResponse = bulkShardResponse.getResponses()[idx];

                    assert bulkItemRequest.id() == bulkItemResponse.getItemId() : "Bulk items were returned out of order";

                    if (bulkItemResponse.isFailed()) {
                        String failureStoreReference = getRedirectTarget(bulkItemRequest.request(), getClusterState().metadata());
                        if (failureStoreReference != null) {
                            addDocumentToRedirectRequests(bulkItemRequest, bulkItemResponse.getFailure().getCause(), failureStoreReference);
                        }

                        // Check for any previously recorded responses in the buffer since this might be a failure during a redirect
                        BulkItemResponse response = responses.get(bulkItemResponse.getItemId());
                        if (response != null) {
                            // Response already recorded. We should only be here if the existing response is a failure and
                            // we are encountering a new failure while redirecting.
                            assert response.isFailed() : "Attempting to overwrite successful bulk item result with a failure";
                            response.getFailure().getCause().addSuppressed(bulkItemResponse.getFailure().getCause());
                            bulkItemResponse = response;
                        }
                    } else {
                        bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                    }
                    responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                }
                completeShardOperation();
            }

            @Override
            public void onFailure(Exception e) {
                // create failures for all relevant requests
                for (BulkItemRequest request : bulkShardRequest.items()) {
                    final String indexName = request.index();
                    DocWriteRequest<?> docWriteRequest = request.request();

                    String failureStoreReference = getRedirectTarget(docWriteRequest, getClusterState().metadata());
                    if (failureStoreReference != null) {
                        addDocumentToRedirectRequests(request, e, failureStoreReference);
                    }

                    // Check for any previously recorded responses in the buffer since this might be a failure during a redirect
                    BulkItemResponse response = responses.get(request.id());
                    if (response == null) {
                        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexName, docWriteRequest.id(), e);
                        response = BulkItemResponse.failure(request.id(), docWriteRequest.opType(), failure);
                    } else {
                        // Response already recorded. We should only be here if the existing response is a failure and
                        // we are encountering a new failure while redirecting.
                        assert response.isFailed() : "Attempting to overwrite successful bulk item result with a failure";
                        response.getFailure().getCause().addSuppressed(e);
                    }
                    // Always replace the item in the responses for thread visibility of any mutations
                    responses.set(request.id(), response);
                }
                completeShardOperation();
            }

            private void completeShardOperation() {
                // Clear our handle on the cluster state to allow it to be cleaned up
                clusterState = null;
                releaseOnFinish.close();
            }
        });
    }

    /**
     * Determines if the write request can be redirected if it fails. Write requests can be redirected IFF they are targeting a data stream
     * with a failure store and are not already redirected themselves. If the document can be redirected, the data stream name to use for
     * the redirection is returned.
     *
     * @param docWriteRequest the write request to check
     * @param metadata cluster state metadata for resolving index abstractions
     * @return a data stream name if the write request points to a data stream that has the failure store enabled,
     *     or {@code null} if it does
     */
    private static String getRedirectTarget(DocWriteRequest<?> docWriteRequest, Metadata metadata) {
        // Feature flag guard
        if (DataStream.isFailureStoreEnabled() == false) {
            return null;
        }
        // Do not resolve a failure store for documents that were already headed to one
        if (docWriteRequest instanceof IndexRequest indexRequest && indexRequest.isWriteToFailureStore() == true) {
            return null;
        }
        // If there is no index abstraction, then the request is using a pattern of some sort, which data streams do not support
        IndexAbstraction ia = metadata.getIndicesLookup().get(docWriteRequest.index());
        if (ia == null) {
            return null;
        }
        if (ia.isDataStreamRelated()) {
            // The index abstraction could be an alias. Alias abstractions (even for data streams) only keep track of which _index_ they
            // will write to, not which _data stream_.
            // We work backward to find the data stream from the concrete write index to cover this case.
            Index concreteIndex = ia.getWriteIndex();
            IndexAbstraction writeIndexAbstraction = metadata.getIndicesLookup().get(concreteIndex.getName());
            DataStream parentDataStream = writeIndexAbstraction.getParentDataStream();
            if (parentDataStream != null && parentDataStream.isFailureStore()) {
                // Keep the data stream name around to resolve the redirect to failure store if the shard level request fails.
                return parentDataStream.getName();
            }
        }
        return null;
    }

    /**
     * Marks a failed bulk item for redirection. At the end of the first round of shard requests, any documents in the
     * redirect list are processed to their final destinations.
     *
     * @param request The bulk item request that failed
     * @param cause The exception for the experienced the failure
     * @param failureStoreReference The data stream that contains the failure store for this item
     */
    private void addDocumentToRedirectRequests(BulkItemRequest request, Exception cause, String failureStoreReference) {
        // Convert the document into a failure document
        IndexRequest failureStoreRequest;
        try {
            failureStoreRequest = FailureStoreDocument.transformFailedRequest(
                TransportBulkAction.getIndexWriteRequest(request.request()),
                cause,
                failureStoreReference,
                threadPool::absoluteTimeInMillis
            );
        } catch (IOException ioException) {
            logger.debug(
                () -> "Could not transform failed bulk request item into failure store document. Attempted for ["
                    + request.request().opType()
                    + ": index="
                    + request.index()
                    + "; id="
                    + request.request().id()
                    + "; bulk_slot="
                    + request.id()
                    + "] Proceeding with failing the original.",
                ioException
            );
            // Suppress and do not redirect
            cause.addSuppressed(ioException);
            return;
        }

        // Store for second phase
        BulkItemRequest redirected = new BulkItemRequest(request.id(), failureStoreRequest);
        failureStoreRedirects.add(redirected);
    }

    private boolean handleBlockExceptions(ClusterState state, Runnable retryOperation, Consumer<Exception> onClusterBlocked) {
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
        if (blockException != null) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked, scheduling a retry", blockException);
                retry(blockException, retryOperation, onClusterBlocked);
            } else {
                onClusterBlocked.accept(blockException);
            }
            return true;
        }
        return false;
    }

    void retry(Exception failure, final Runnable operation, final Consumer<Exception> onClusterBlocked) {
        assert failure != null;
        if (observer.isTimedOut()) {
            // we are running as a last attempt after a timeout has happened. don't retry
            onClusterBlocked.accept(failure);
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
                // TODO: There is likely very little we can do about this, and our time in this JVM is likely short,
                //  should we just get out of here ASAP?
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
                threadPool.executor(executorName).submit(operation);
            }
        });
    }

    private boolean addFailureIfRequiresAliasAndAliasIsMissing(DocWriteRequest<?> request, int idx, final Metadata metadata) {
        if (request.isRequireAlias() && (metadata.hasAlias(request.index()) == false)) {
            Exception exception = new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + request.index() + "] is not an alias",
                request.index()
            );
            addFailure(request, idx, request.index(), exception);
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
            addFailure(request, idx, request.index(), exception);
            return true;
        }
        return false;
    }

    private boolean addFailureIfIndexIsClosed(DocWriteRequest<?> request, Index concreteIndex, int idx, final Metadata metadata) {
        IndexMetadata indexMetadata = metadata.getIndexSafe(concreteIndex);
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            addFailure(request, idx, request.index(), new IndexClosedException(concreteIndex));
            return true;
        }
        return false;
    }

    private boolean addFailureIfIndexCannotBeCreated(DocWriteRequest<?> request, int idx) {
        IndexNotFoundException cannotCreate = indicesThatCannotBeCreated.get(request.index());
        if (cannotCreate != null) {
            addFailure(request, idx, request.index(), cannotCreate);
            return true;
        }
        return false;
    }

    private void addFailure(DocWriteRequest<?> request, int idx, String index, Exception exception) {
        BulkItemResponse bulkItemResponse = responses.get(idx);
        if (bulkItemResponse == null) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, request.id(), exception);
            bulkItemResponse = BulkItemResponse.failure(idx, request.opType(), failure);
        } else {
            // Response already recorded. We should only be here if the existing response is a failure and
            // we are encountering a new failure while redirecting.
            assert bulkItemResponse.isFailed() : "Attempting to overwrite successful bulk item result with a failure";
            bulkItemResponse.getFailure().getCause().addSuppressed(exception);
        }
        // Always replace the item in the responses for thread visibility of any mutations
        responses.set(idx, bulkItemResponse);
        // make sure the request gets never processed again
        bulkRequest.requests.set(idx, null);
    }

    /**
     * Resolves and caches index and routing abstractions to more efficiently group write requests into shards.
     */
    private static class ConcreteIndices {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, IndexAbstraction> indexAbstractions = new HashMap<>();
        private final Map<Index, IndexRouting> routings = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        /**
         * Resolves the index abstraction that the write request is targeting, potentially obtaining it from a cache. This instance isn't
         * fully resolved, meaning that {@link IndexAbstraction#getWriteIndex()} should be invoked in order to get concrete write index.
         *
         * @param request a write request
         * @return the index abstraction that the write request is targeting
         */
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

        /**
         * Determines which routing strategy to use for a document being written to the provided index, potentially obtaining the result
         * from a cache.
         * @param index the index to determine routing strategy for
         * @return an {@link IndexRouting} object to use for assigning a write request to a shard
         */
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
