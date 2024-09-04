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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * This is an abstract base class for bulk actions. It traverses all indices that the request gets routed to, executes all applicable
 * pipelines, and then delegates to the concrete implementation of #doInternalExecute to actually index the data.
 */
public abstract class TransportAbstractBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {
    private static final Logger logger = LogManager.getLogger(TransportAbstractBulkAction.class);

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final IndexingPressure indexingPressure;
    protected final SystemIndices systemIndices;
    private final IngestService ingestService;
    private final IngestActionForwarder ingestForwarder;
    protected final LongSupplier relativeTimeNanosProvider;
    protected final Executor writeExecutor;
    protected final Executor systemWriteExecutor;
    private final ActionType<BulkResponse> bulkAction;

    public TransportAbstractBulkAction(
        ActionType<BulkResponse> action,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<BulkRequest> requestReader,
        ThreadPool threadPool,
        ClusterService clusterService,
        IngestService ingestService,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        LongSupplier relativeTimeNanosProvider
    ) {
        super(action.name(), transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.indexingPressure = indexingPressure;
        this.systemIndices = systemIndices;
        this.writeExecutor = threadPool.executor(ThreadPool.Names.WRITE);
        this.systemWriteExecutor = threadPool.executor(ThreadPool.Names.SYSTEM_WRITE);
        this.ingestForwarder = new IngestActionForwarder(transportService);
        clusterService.addStateApplier(this.ingestForwarder);
        this.relativeTimeNanosProvider = relativeTimeNanosProvider;
        this.bulkAction = action;
    }

    @Override
    protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        /*
         * This is called on the Transport thread so we can check the indexing
         * memory pressure *quickly* but we don't want to keep the transport
         * thread busy. Then, as soon as we have the indexing pressure in we fork
         * to one of the write thread pools. We do this because juggling the
         * bulk request can get expensive for a few reasons:
         * 1. Figuring out which shard should receive a bulk request might require
         *    parsing the _source.
         * 2. When dispatching the sub-requests to shards we may have to compress
         *    them. LZ4 is super fast, but slow enough that it's best not to do it
         *    on the transport thread, especially for large sub-requests.
         *
         * We *could* detect these cases and only fork in then, but that is complex
         * to get right and the fork is fairly low overhead.
         */
        final int indexingOps = bulkRequest.numberOfActions();
        final long indexingBytes = bulkRequest.ramBytesUsed();
        final boolean isOnlySystem = TransportBulkAction.isOnlySystem(
            bulkRequest,
            clusterService.state().metadata().getIndicesLookup(),
            systemIndices
        );
        final Releasable releasable = indexingPressure.markCoordinatingOperationStarted(indexingOps, indexingBytes, isOnlySystem);
        final ActionListener<BulkResponse> releasingListener = ActionListener.runBefore(listener, releasable::close);
        final Executor executor = isOnlySystem ? systemWriteExecutor : writeExecutor;
        ensureClusterStateThenForkAndExecute(task, bulkRequest, executor, releasingListener);
    }

    private void ensureClusterStateThenForkAndExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> releasingListener
    ) {
        final ClusterState initialState = clusterService.state();
        final ClusterBlockException blockException = initialState.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
        if (blockException != null) {
            if (false == blockException.retryable()) {
                releasingListener.onFailure(blockException);
                return;
            }
            logger.trace("cluster is blocked, waiting for it to recover", blockException);
            final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
                initialState,
                clusterService,
                bulkRequest.timeout(),
                logger,
                threadPool.getThreadContext()
            );
            clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    forkAndExecute(task, bulkRequest, executor, releasingListener);
                }

                @Override
                public void onClusterServiceClose() {
                    releasingListener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    releasingListener.onFailure(blockException);
                }
            }, newState -> false == newState.blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.WRITE));
        } else {
            forkAndExecute(task, bulkRequest, executor, releasingListener);
        }
    }

    private void forkAndExecute(Task task, BulkRequest bulkRequest, Executor executor, ActionListener<BulkResponse> releasingListener) {
        executor.execute(new ActionRunnable<>(releasingListener) {
            @Override
            protected void doRun() {
                applyPipelinesAndDoInternalExecute(task, bulkRequest, executor, releasingListener);
            }
        });
    }

    private boolean applyPipelines(Task task, BulkRequest bulkRequest, Executor executor, ActionListener<BulkResponse> listener) {
        boolean hasIndexRequestsWithPipelines = false;
        final Metadata metadata = clusterService.state().getMetadata();
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests) {
            IndexRequest indexRequest = getIndexWriteRequest(actionRequest);
            if (indexRequest != null) {
                IngestService.resolvePipelinesAndUpdateIndexRequest(actionRequest, indexRequest, metadata);
                hasIndexRequestsWithPipelines |= IngestService.hasPipeline(indexRequest);
            }

            if (actionRequest instanceof IndexRequest ir) {
                if (ir.getAutoGeneratedTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
                    throw new IllegalArgumentException("autoGeneratedTimestamp should not be set externally");
                }
            }
        }

        if (hasIndexRequestsWithPipelines) {
            // this method (doExecute) will be called again, but with the bulk requests updated from the ingest node processing but
            // also with IngestService.NOOP_PIPELINE_NAME on each request. This ensures that this on the second time through this method,
            // this path is never taken.
            ActionListener.run(listener, l -> {
                if (Assertions.ENABLED) {
                    final boolean arePipelinesResolved = bulkRequest.requests()
                        .stream()
                        .map(TransportBulkAction::getIndexWriteRequest)
                        .filter(Objects::nonNull)
                        .allMatch(IndexRequest::isPipelineResolved);
                    assert arePipelinesResolved : bulkRequest;
                }
                if (clusterService.localNode().isIngestNode()) {
                    processBulkIndexIngestRequest(task, bulkRequest, executor, metadata, l);
                } else {
                    ingestForwarder.forwardIngestRequest(bulkAction, bulkRequest, l);
                }
            });
            return true;
        }
        return false;
    }

    private void processBulkIndexIngestRequest(
        Task task,
        BulkRequest original,
        Executor executor,
        Metadata metadata,
        ActionListener<BulkResponse> listener
    ) {
        final long ingestStartTimeInNanos = relativeTimeNanos();
        final BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        getIngestService(original).executeBulkRequest(
            original.numberOfActions(),
            () -> bulkRequestModifier,
            bulkRequestModifier::markItemAsDropped,
            (indexName) -> resolveFailureStore(indexName, metadata, threadPool.absoluteTimeInMillis()),
            bulkRequestModifier::markItemForFailureStore,
            bulkRequestModifier::markItemAsFailed,
            (originalThread, exception) -> {
                if (exception != null) {
                    logger.debug("failed to execute pipeline for a bulk request", exception);
                    listener.onFailure(exception);
                } else {
                    long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeNanos() - ingestStartTimeInNanos);
                    BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
                    ActionListener<BulkResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(
                        ingestTookInMillis,
                        listener
                    );
                    if (bulkRequest.requests().isEmpty()) {
                        // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                        // so we stop and send an empty response back to the client.
                        // (this will happen if pre-processing all items in the bulk failed)
                        actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
                    } else {
                        ActionRunnable<BulkResponse> runnable = new ActionRunnable<>(actionListener) {
                            @Override
                            protected void doRun() {
                                applyPipelinesAndDoInternalExecute(task, bulkRequest, executor, actionListener);
                            }

                            @Override
                            public boolean isForceExecution() {
                                // If we fork back to a write thread we **not** should fail, because tp queue is full.
                                // (Otherwise the work done during ingest will be lost)
                                // It is okay to force execution here. Throttling of write requests happens prior to
                                // ingest when a node receives a bulk request.
                                return true;
                            }
                        };
                        // If a processor went async and returned a response on a different thread then
                        // before we continue the bulk request we should fork back on a write thread:
                        if (originalThread == Thread.currentThread()) {
                            runnable.run();
                        } else {
                            executor.execute(runnable);
                        }
                    }
                }
            },
            executor
        );
    }

    /**
     * Determines if an index name is associated with either an existing data stream or a template
     * for one that has the failure store enabled.
     *
     * @param indexName The index name to check.
     * @param metadata Cluster state metadata.
     * @param epochMillis A timestamp to use when resolving date math in the index name.
     * @return true if this is not a simulation, and the given index name corresponds to a data stream with a failure store
     * or if it matches a template that has a data stream failure store enabled. Returns false if the index name corresponds to a
     * data stream, but it doesn't have the failure store enabled. Returns null when it doesn't correspond to a data stream.
     */
    protected abstract Boolean resolveFailureStore(String indexName, Metadata metadata, long epochMillis);

    /**
     * Retrieves the {@link IndexRequest} from the provided {@link DocWriteRequest} for index or upsert actions.  Upserts are
     * modeled as {@link IndexRequest} inside the {@link UpdateRequest}. Ignores {@link org.elasticsearch.action.delete.DeleteRequest}'s
     *
     * @param docWriteRequest The request to find the {@link IndexRequest}
     * @return the found {@link IndexRequest} or {@code null} if one can not be found.
     */
    public static IndexRequest getIndexWriteRequest(DocWriteRequest<?> docWriteRequest) {
        IndexRequest indexRequest = null;
        if (docWriteRequest instanceof IndexRequest) {
            indexRequest = (IndexRequest) docWriteRequest;
        } else if (docWriteRequest instanceof UpdateRequest updateRequest) {
            indexRequest = updateRequest.docAsUpsert() ? updateRequest.doc() : updateRequest.upsertRequest();
        }
        return indexRequest;
    }

    /*
     * This returns the IngestService to be used for the given request. The default implementation ignores the request and always returns
     * the same ingestService, but child classes might use information in the request in creating an IngestService specific to that request.
     */
    protected IngestService getIngestService(BulkRequest request) {
        return ingestService;
    }

    protected long relativeTimeNanos() {
        return relativeTimeNanosProvider.getAsLong();
    }

    protected long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTimeNanos() - startTimeNanos);
    }

    private void applyPipelinesAndDoInternalExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener
    ) {
        final long relativeStartTimeNanos = relativeTimeNanos();
        if (applyPipelines(task, bulkRequest, executor, listener) == false) {
            doInternalExecute(task, bulkRequest, executor, listener, relativeStartTimeNanos);
        }
    }

    /**
     * This method creates any missing resources and actually applies the BulkRequest to the relevant indices
     * @param task The task in which this work is being done
     * @param bulkRequest The BulkRequest of changes to make to indices
     * @param executor The executor for the thread pool in which the work is to be done
     * @param listener The listener to be notified of results
     * @param relativeStartTimeNanos The relative start time of this bulk load, to be used in computing the time taken for the BulkResponse
     */
    protected abstract void doInternalExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        long relativeStartTimeNanos
    );

}
