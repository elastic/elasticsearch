/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.LazyRolloverAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SimulateIngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class TransportSimulateBulkAction extends HandledTransportAction<SimulateBulkRequest, BulkResponse> {
//    public static final String NAME = "indices:data/read/bulk";
//    public static final ActionType<BulkResponse> TYPE = new ActionType<>(NAME);
    private final ActionType<BulkResponse> bulkAction;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IngestService ingestService;
    private final FeatureService featureService;
    private final LongSupplier relativeTimeProvider;
    private final IngestActionForwarder ingestForwarder;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;

    private final Executor writeExecutor;
    private final Executor systemWriteExecutor;

    @Inject
    public TransportSimulateBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        FeatureService featureService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(SimulateBulkAction.INSTANCE.name(), transportService, actionFilters, SimulateBulkRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE);
        LongSupplier relativeTimeProvider = System::nanoTime;
        this.bulkAction = SimulateBulkAction.INSTANCE;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.featureService = featureService;
        this.relativeTimeProvider = relativeTimeProvider;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        clusterService.addStateApplier(this.ingestForwarder);
        this.writeExecutor = threadPool.executor(ThreadPool.Names.WRITE);
        this.systemWriteExecutor = threadPool.executor(ThreadPool.Names.SYSTEM_WRITE);
    }

    /*
     * This overrides indexData in TransportBulkAction in order to _not_ actually create any indices or index any data. Instead, each
     * request gets a corresponding CREATE response, using information from the request.
     */
    protected void createMissingIndicesAndIndexData(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        Map<String, Boolean> indicesToAutoCreate,
        Set<String> dataStreamsToRollover,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        long startTime
    ) {
        assert bulkRequest instanceof SimulateBulkRequest;
        createMissingIndicesAndIndexData2(task, bulkRequest, executor, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                BulkItemResponse[] originalResponses = bulkResponse.getItems();
                final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                    DocWriteRequest<?> request = bulkRequest.requests.get(i);
                    BulkItemResponse originalResponse = originalResponses[i];
                    Exception exception = originalResponse.isFailed() ? originalResponse.getFailure().getCause() : null;
                    assert request != null : "A request was unexpectedly set to null. Simulate action never set requests to null";
                    // This action is only every called with IndexRequests:
                    assert request instanceof IndexRequest : "expected IndexRequest but got " + request.getClass();
                    BulkItemResponse updatedResponse = BulkItemResponse.success(
                        originalResponse.getItemId(),
                        originalResponse.getOpType(),
                        new SimulateIndexResponse(
                            request.id(),
                            request.index(),
                            request.version(),
                            ((IndexRequest) request).source(),
                            ((IndexRequest) request).getContentType(),
                            ((IndexRequest) request).getExecutedPipelines(),
                            exception
                        )
                    );
                    responses.set(i, updatedResponse);
                }
                listener.onResponse(
                    new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime))
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, indicesToAutoCreate, dataStreamsToRollover, indicesThatCannotBeCreated, startTime);
    }

    /*
     * This overrides TransportSimulateBulkAction's getIngestService to allow us to provide an IngestService that handles pipeline
     * substitutions defined in the request.
     */
    protected IngestService getIngestService(BulkRequest request) {
        return new SimulateIngestService(ingestService, request);
    }

    @Override
    protected void doExecute(Task task, SimulateBulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        final boolean isOnlySystem = isOnlySystem(bulkRequest, clusterService.state().metadata().getIndicesLookup(), systemIndices);
        final Executor executor = isOnlySystem ? systemWriteExecutor : writeExecutor;
        ensureClusterStateThenForkAndExecute(task, bulkRequest, executor, listener);
    }

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

    public static <Response extends ReplicationResponse & WriteResponse> ActionListener<BulkResponse> unwrappingSingleItemBulkResponse(
        final ActionListener<Response> listener
    ) {
        return listener.delegateFailureAndWrap((l, bulkItemResponses) -> {
            assert bulkItemResponses.getItems().length == 1 : "expected exactly one item in bulk response";
            final BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                @SuppressWarnings("unchecked")
                final Response response = (Response) bulkItemResponse.getResponse();
                l.onResponse(response);
            } else {
                l.onFailure(bulkItemResponse.getFailure().getCause());
            }
        });
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
                doInternalExecute(task, bulkRequest, executor, releasingListener);
            }
        });
    }

    protected void doInternalExecute(Task task, BulkRequest bulkRequest, Executor executor, ActionListener<BulkResponse> listener) {
        final long startTime = relativeTime();

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
                        .map(TransportSimulateBulkAction::getIndexWriteRequest)
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
            return;
        }

        // Attempt to create all the indices that we're going to need during the bulk before we start.
        // Step 1: collect all the indices in the request
        final Map<String, TransportSimulateBulkAction.ReducedRequestInfo> indices = bulkRequest.requests.stream()
            // delete requests should not attempt to create the index (if the index does not
            // exist), unless an external versioning is used
            .filter(
                request -> request.opType() != DocWriteRequest.OpType.DELETE
                    || request.versionType() == VersionType.EXTERNAL
                    || request.versionType() == VersionType.EXTERNAL_GTE
            )
            .collect(
                Collectors.toMap(
                    DocWriteRequest::index,
                    request -> TransportSimulateBulkAction.ReducedRequestInfo.of(request.isRequireAlias(), request.isRequireDataStream()),
                    (existing, updated) -> TransportSimulateBulkAction.ReducedRequestInfo.of(
                        existing.isRequireAlias || updated.isRequireAlias,
                        existing.isRequireDataStream || updated.isRequireDataStream
                    )
                )
            );

        // Step 2: filter the list of indices to find those that don't currently exist.
        final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();
        final ClusterState state = clusterService.state();
        Map<String, Boolean> indicesToAutoCreate = indices.entrySet()
            .stream()
            .filter(entry -> indexNameExpressionResolver.hasIndexAbstraction(entry.getKey(), state) == false)
            // We should only auto create if we are not requiring it to be an alias
            .filter(entry -> entry.getValue().isRequireAlias == false)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().isRequireDataStream));

        // Step 3: Collect all the data streams that need to be rolled over before writing
        Set<String> dataStreamsToBeRolledOver = featureService.clusterHasFeature(state, LazyRolloverAction.DATA_STREAM_LAZY_ROLLOVER)
            ? indices.keySet().stream().filter(target -> {
            DataStream dataStream = state.metadata().dataStreams().get(target);
            return dataStream != null && dataStream.rolloverOnWrite();
        }).collect(Collectors.toSet())
            : Set.of();

        // Step 4: create all the indices that are missing, if there are any missing. start the bulk after all the creates come back.
        createMissingIndicesAndIndexData(
            task,
            bulkRequest,
            executor,
            listener,
            indicesToAutoCreate,
            dataStreamsToBeRolledOver,
            indicesThatCannotBeCreated,
            startTime
        );
    }

    /*
     * This method is responsible for creating any missing indices, rolling over a data stream when needed and then
     *  indexing the data in the BulkRequest
     */
    protected void createMissingIndicesAndIndexData2(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        Map<String, Boolean> indicesToAutoCreate,
        Set<String> dataStreamsToBeRolledOver,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        long startTime
    ) {
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        // Optimizing when there are no prerequisite actions
        executeBulk(task, bulkRequest, startTime, listener, executor, responses, indicesThatCannotBeCreated);
    }

    static boolean isOnlySystem(BulkRequest request, SortedMap<String, IndexAbstraction> indicesLookup, SystemIndices systemIndices) {
        return request.getIndices().stream().allMatch(indexName -> isSystemIndex(indicesLookup, systemIndices, indexName));
    }

    private static boolean isSystemIndex(SortedMap<String, IndexAbstraction> indicesLookup, SystemIndices systemIndices, String indexName) {
        final IndexAbstraction abstraction = indicesLookup.get(indexName);
        if (abstraction != null) {
            return abstraction.isSystem();
        } else {
            return systemIndices.isSystemIndex(indexName);
        }
    }

    private static boolean setResponseFailureIfIndexMatches(
        AtomicArray<BulkItemResponse> responses,
        int idx,
        DocWriteRequest<?> request,
        String index,
        Exception e
    ) {
        if (index.equals(request.index())) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.id(), e);
            responses.set(idx, BulkItemResponse.failure(idx, request.opType(), failure));
            return true;
        }
        return false;
    }

    protected long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    private enum ReducedRequestInfo {

        REQUIRE_ALIAS_AND_DATA_STREAM(true, true),
        REQUIRE_ALIAS_NOT_DATA_STREAM(true, false),

        REQUIRE_DATA_STREAM_NOT_ALIAS(false, true),
        REQUIRE_NOTHING(false, false);

        private final boolean isRequireAlias;
        private final boolean isRequireDataStream;

        ReducedRequestInfo(boolean isRequireAlias, boolean isRequireDataStream) {
            this.isRequireAlias = isRequireAlias;
            this.isRequireDataStream = isRequireDataStream;
        }

        static TransportSimulateBulkAction.ReducedRequestInfo of(boolean isRequireAlias, boolean isRequireDataStream) {
            if (isRequireAlias) {
                return isRequireDataStream ? REQUIRE_ALIAS_AND_DATA_STREAM : REQUIRE_ALIAS_NOT_DATA_STREAM;
            }
            return isRequireDataStream ? REQUIRE_DATA_STREAM_NOT_ALIAS : REQUIRE_NOTHING;
        }

    }

    void executeBulk(
        Task task,
        BulkRequest bulkRequest,
        long startTimeNanos,
        ActionListener<BulkResponse> listener,
        Executor executor,
        AtomicArray<BulkItemResponse> responses,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated
    ) {
        new SimulateBulkOperation(
            task,
            threadPool,
            executor,
            clusterService,
            bulkRequest,
            client,
            responses,
            indicesThatCannotBeCreated,
            indexNameExpressionResolver,
            relativeTimeProvider,
            startTimeNanos,
            listener
        ).run();
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }

    private void processBulkIndexIngestRequest(
        Task task,
        BulkRequest original,
        Executor executor,
        Metadata metadata,
        ActionListener<BulkResponse> listener
    ) {
        final long ingestStartTimeInNanos = System.nanoTime();
        final BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        getIngestService(original).executeBulkRequest(
            original.numberOfActions(),
            () -> bulkRequestModifier,
            bulkRequestModifier::markItemAsDropped,
            (indexName) -> false,
            bulkRequestModifier::markItemForFailureStore,
            bulkRequestModifier::markItemAsFailed,
            (originalThread, exception) -> {
                if (exception != null) {
                    logger.debug("failed to execute pipeline for a bulk request", exception);
                    listener.onFailure(exception);
                } else {
                    long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);
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
                                doInternalExecute(task, bulkRequest, executor, actionListener);
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
     * Determines if an index name is associated with an existing data stream that has a failure store enabled.
     * @param indexName The index name to check.
     * @param metadata Cluster state metadata.
     * @param epochMillis A timestamp to use when resolving date math in the index name.
     * @return true if the given index name corresponds to an existing data stream with a failure store enabled.
     */
    private static Optional<Boolean> resolveFailureStoreFromMetadata(String indexName, Metadata metadata, long epochMillis) {
        if (indexName == null) {
            return Optional.empty();
        }

        // Get index abstraction, resolving date math if it exists
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup()
            .get(IndexNameExpressionResolver.resolveDateMathExpression(indexName, epochMillis));

        // We only store failures if the failure is being written to a data stream,
        // not when directly writing to backing indices/failure stores
        if (indexAbstraction == null || indexAbstraction.isDataStreamRelated() == false) {
            return Optional.empty();
        }

        // Locate the write index for the abstraction, and check if it has a data stream associated with it.
        // This handles alias resolution as well as data stream resolution.
        Index writeIndex = indexAbstraction.getWriteIndex();
        assert writeIndex != null : "Could not resolve write index for resource [" + indexName + "]";
        IndexAbstraction writeAbstraction = metadata.getIndicesLookup().get(writeIndex.getName());
        DataStream targetDataStream = writeAbstraction.getParentDataStream();

        // We will store the failure if the write target belongs to a data stream with a failure store.
        return Optional.of(targetDataStream != null && targetDataStream.isFailureStoreEnabled());
    }

    /**
     * Determines if an index name is associated with an index template that has a data stream failure store enabled.
     * @param indexName The index name to check.
     * @param metadata Cluster state metadata.
     * @return true if the given index name corresponds to an index template with a data stream failure store enabled.
     */
    private static Optional<Boolean> resolveFailureStoreFromTemplate(String indexName, Metadata metadata) {
        if (indexName == null) {
            return Optional.empty();
        }

        // Check to see if the index name matches any templates such that an index would have been attributed
        // We don't check v1 templates at all because failure stores can only exist on data streams via a v2 template
        String template = MetadataIndexTemplateService.findV2Template(metadata, indexName, false);
        if (template != null) {
            // Check if this is a data stream template or if it is just a normal index.
            ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(template);
            if (composableIndexTemplate.getDataStreamTemplate() != null) {
                // Check if the data stream has the failure store enabled
                return Optional.of(composableIndexTemplate.getDataStreamTemplate().hasFailureStore());
            }
        }

        // Could not locate a failure store via template
        return Optional.empty();
    }
}
