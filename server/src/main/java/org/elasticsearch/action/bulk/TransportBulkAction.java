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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.LazyRolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Groups bulk request items by shard, optionally creating non-existent indices and
 * delegates to {@link TransportShardBulkAction} for shard-level bulk execution
 */
public class TransportBulkAction extends TransportAbstractBulkAction {

    public static final String NAME = "indices:data/write/bulk";
    public static final ActionType<BulkResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportBulkAction.class);
    public static final String LAZY_ROLLOVER_ORIGIN = "lazy_rollover";

    private final FeatureService featureService;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final OriginSettingClient rolloverClient;
    private final FailureStoreMetrics failureStoreMetrics;

    @Inject
    public TransportBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        FeatureService featureService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        FailureStoreMetrics failureStoreMetrics
    ) {
        this(
            threadPool,
            transportService,
            clusterService,
            ingestService,
            featureService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            indexingPressure,
            systemIndices,
            threadPool::relativeTimeInNanos,
            failureStoreMetrics
        );
    }

    public TransportBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        FeatureService featureService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        LongSupplier relativeTimeProvider,
        FailureStoreMetrics failureStoreMetrics
    ) {
        this(
            TYPE,
            BulkRequest::new,
            threadPool,
            transportService,
            clusterService,
            ingestService,
            featureService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            indexingPressure,
            systemIndices,
            relativeTimeProvider,
            failureStoreMetrics
        );
    }

    TransportBulkAction(
        ActionType<BulkResponse> bulkAction,
        Writeable.Reader<BulkRequest> requestReader,
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        FeatureService featureService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        LongSupplier relativeTimeProvider,
        FailureStoreMetrics failureStoreMetrics
    ) {
        super(
            bulkAction,
            transportService,
            actionFilters,
            requestReader,
            threadPool,
            clusterService,
            ingestService,
            indexingPressure,
            systemIndices,
            relativeTimeProvider
        );
        Objects.requireNonNull(relativeTimeProvider);
        this.featureService = featureService;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.rolloverClient = new OriginSettingClient(client, LAZY_ROLLOVER_ORIGIN);
        this.failureStoreMetrics = failureStoreMetrics;
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

    @Override
    protected void doInternalExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        long relativeStartTimeNanos
    ) {
        trackIndexRequests(bulkRequest);

        Map<String, CreateIndexRequest> indicesToAutoCreate = new HashMap<>();
        Set<String> dataStreamsToBeRolledOver = new HashSet<>();
        Set<String> failureStoresToBeRolledOver = new HashSet<>();
        populateMissingTargets(bulkRequest, indicesToAutoCreate, dataStreamsToBeRolledOver, failureStoresToBeRolledOver);

        createMissingIndicesAndIndexData(
            task,
            bulkRequest,
            executor,
            listener,
            indicesToAutoCreate,
            dataStreamsToBeRolledOver,
            failureStoresToBeRolledOver,
            relativeStartTimeNanos
        );
    }

    /**
     * Track the number of index requests in our APM metrics. We'll track almost all docs here (pipeline or no pipeline,
     * failure store or original), but some docs don't reach this place (dropped and rejected docs), so we increment for those docs in
     * different places.
     */
    private void trackIndexRequests(BulkRequest bulkRequest) {
        final Metadata metadata = clusterService.state().metadata();
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            if (request instanceof IndexRequest == false) {
                continue;
            }
            String resolvedIndexName = IndexNameExpressionResolver.resolveDateMathExpression(request.index());
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(resolvedIndexName);
            DataStream dataStream = DataStream.resolveDataStream(indexAbstraction, metadata);
            // We only track index requests into data streams.
            if (dataStream != null) {
                failureStoreMetrics.incrementTotal(dataStream.getName());
            }
        }
    }

    /**
     * Determine all the targets (i.e. indices, data streams, failure stores) that require an action before we can proceed with the bulk
     * request. Indices might need to be created, and data streams and failure stores might need to be rolled over when they're marked
     * for lazy rollover.
     *
     * @param bulkRequest the bulk request
     * @param indicesToAutoCreate a map of index names to their creation request that need to be auto-created
     * @param dataStreamsToBeRolledOver a set of data stream names that were marked for lazy rollover and thus need to be rolled over now
     * @param failureStoresToBeRolledOver a set of data stream names whose failure store was marked for lazy rollover and thus need to be
     * rolled over now
     */
    private void populateMissingTargets(
        BulkRequest bulkRequest,
        Map<String, CreateIndexRequest> indicesToAutoCreate,
        Set<String> dataStreamsToBeRolledOver,
        Set<String> failureStoresToBeRolledOver
    ) {
        ClusterState state = clusterService.state();
        // A map for memorizing which indices exist.
        Map<String, Boolean> indexExistence = new HashMap<>();
        Function<String, Boolean> indexExistenceComputation = (index) -> indexNameExpressionResolver.hasIndexAbstraction(index, state);
        boolean lazyRolloverFeature = featureService.clusterHasFeature(state, LazyRolloverAction.DATA_STREAM_LAZY_ROLLOVER);
        boolean lazyRolloverFailureStoreFeature = DataStream.isFailureStoreFeatureFlagEnabled();
        Set<String> indicesThatRequireAlias = new HashSet<>();

        for (DocWriteRequest<?> request : bulkRequest.requests) {
            // Delete requests should not attempt to create the index (if the index does not exist), unless an external versioning is used.
            if (request.opType() == OpType.DELETE
                && request.versionType() != VersionType.EXTERNAL
                && request.versionType() != VersionType.EXTERNAL_GTE) {
                continue;
            }
            boolean writeToFailureStore = request instanceof IndexRequest indexRequest && indexRequest.isWriteToFailureStore();
            boolean indexExists = indexExistence.computeIfAbsent(request.index(), indexExistenceComputation);
            if (indexExists == false) {
                // We should only auto-create an index if _none_ of the requests are requiring it to be an alias.
                if (request.isRequireAlias()) {
                    // Remember that this request required this index to be an alias.
                    if (indicesThatRequireAlias.add(request.index())) {
                        // If we didn't already know that, we remove the index from the list of indices to create (if present).
                        indicesToAutoCreate.remove(request.index());
                    }
                } else if (indicesThatRequireAlias.contains(request.index()) == false) {
                    CreateIndexRequest createIndexRequest = indicesToAutoCreate.get(request.index());
                    // Create a new CreateIndexRequest if we didn't already have one.
                    if (createIndexRequest == null) {
                        createIndexRequest = new CreateIndexRequest(request.index()).cause("auto(bulk api)")
                            .masterNodeTimeout(bulkRequest.timeout())
                            .requireDataStream(request.isRequireDataStream())
                            // If this IndexRequest is directed towards a failure store, but the data stream doesn't exist, we initialize
                            // the failure store on data stream creation instead of lazily.
                            .initializeFailureStore(writeToFailureStore);
                        indicesToAutoCreate.put(request.index(), createIndexRequest);
                    } else {
                        // Track whether one of the index requests in this bulk request requires the target to be a data stream.
                        if (createIndexRequest.isRequireDataStream() == false && request.isRequireDataStream()) {
                            createIndexRequest.requireDataStream(true);
                        }
                        // Track whether one of the index requests in this bulk request is directed towards a failure store.
                        if (createIndexRequest.isInitializeFailureStore() == false && writeToFailureStore) {
                            createIndexRequest.initializeFailureStore(true);
                        }
                    }
                }
            }
            // Determine which data streams and failure stores need to be rolled over.
            if (lazyRolloverFeature) {
                DataStream dataStream = state.metadata().dataStreams().get(request.index());
                if (dataStream != null) {
                    if (writeToFailureStore == false && dataStream.getBackingIndices().isRolloverOnWrite()) {
                        dataStreamsToBeRolledOver.add(request.index());
                    } else if (lazyRolloverFailureStoreFeature
                        && writeToFailureStore
                        && dataStream.getFailureIndices().isRolloverOnWrite()) {
                            failureStoresToBeRolledOver.add(request.index());
                        }
                }
            }
        }
    }

    /**
     * This method is responsible for creating any missing indices, rolling over data streams and their failure stores when needed, and then
     * indexing the data in the BulkRequest.
     */
    protected void createMissingIndicesAndIndexData(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        Map<String, CreateIndexRequest> indicesToAutoCreate,
        Set<String> dataStreamsToBeRolledOver,
        Set<String> failureStoresToBeRolledOver,
        long startTimeNanos
    ) {
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        // Optimizing when there are no prerequisite actions
        if (indicesToAutoCreate.isEmpty() && dataStreamsToBeRolledOver.isEmpty() && failureStoresToBeRolledOver.isEmpty()) {
            executeBulk(task, bulkRequest, startTimeNanos, listener, executor, responses, Map.of());
            return;
        }
        final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();
        Runnable executeBulkRunnable = () -> executor.execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                executeBulk(task, bulkRequest, startTimeNanos, listener, executor, responses, indicesThatCannotBeCreated);
            }
        });
        try (RefCountingRunnable refs = new RefCountingRunnable(executeBulkRunnable)) {
            createIndices(bulkRequest, indicesToAutoCreate, indicesThatCannotBeCreated, responses, refs);
            rollOverDataStreams(bulkRequest, dataStreamsToBeRolledOver, false, responses, refs);
            rollOverDataStreams(bulkRequest, failureStoresToBeRolledOver, true, responses, refs);
        }
    }

    private void createIndices(
        BulkRequest bulkRequest,
        Map<String, CreateIndexRequest> indicesToAutoCreate,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated,
        AtomicArray<BulkItemResponse> responses,
        RefCountingRunnable refs
    ) {
        for (Map.Entry<String, CreateIndexRequest> indexEntry : indicesToAutoCreate.entrySet()) {
            final String index = indexEntry.getKey();
            createIndex(indexEntry.getValue(), ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {}

                @Override
                public void onFailure(Exception e) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException indexNotFoundException) {
                        synchronized (indicesThatCannotBeCreated) {
                            indicesThatCannotBeCreated.put(index, indexNotFoundException);
                        }
                    } else if ((cause instanceof ResourceAlreadyExistsException) == false) {
                        // fail all requests involving this index, if create didn't work
                        failRequestsWhenPrerequisiteActionFailed(index, bulkRequest, responses, e);
                    }
                }
            }, refs.acquire()));
        }
    }

    private void rollOverDataStreams(
        BulkRequest bulkRequest,
        Set<String> dataStreamsToBeRolledOver,
        boolean targetFailureStore,
        AtomicArray<BulkItemResponse> responses,
        RefCountingRunnable refs
    ) {
        for (String dataStream : dataStreamsToBeRolledOver) {
            RolloverRequest rolloverRequest = new RolloverRequest(dataStream, null);
            rolloverRequest.masterNodeTimeout(bulkRequest.timeout);
            if (targetFailureStore) {
                rolloverRequest.setIndicesOptions(
                    IndicesOptions.builder(rolloverRequest.indicesOptions())
                        .failureStoreOptions(new IndicesOptions.FailureStoreOptions(false, true))
                        .build()
                );
            }
            // We are executing a lazy rollover because it is an action specialised for this situation, when we want an
            // unconditional and performant rollover.
            rolloverClient.execute(LazyRolloverAction.INSTANCE, rolloverRequest, ActionListener.releaseAfter(new ActionListener<>() {

                @Override
                public void onResponse(RolloverResponse result) {
                    logger.debug(
                        "Data stream{} {} has {} over, the latest index is {}",
                        rolloverRequest.targetsFailureStore() ? " failure store" : "",
                        dataStream,
                        result.isRolledOver() ? "been successfully rolled" : "skipped rolling",
                        result.getNewIndex()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    failRequestsWhenPrerequisiteActionFailed(dataStream, bulkRequest, responses, e);
                }
            }, refs.acquire()));
        }
    }

    /**
     * Fails all requests involving this index or data stream because the prerequisite action failed too.
     */
    private static void failRequestsWhenPrerequisiteActionFailed(
        String target,
        BulkRequest bulkRequest,
        AtomicArray<BulkItemResponse> responses,
        Exception error
    ) {
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> request = bulkRequest.requests.get(i);
            if (request != null && setResponseFailureIfIndexMatches(responses, i, request, target, error)) {
                bulkRequest.requests.set(i, null);
            }
        }
    }

    static void prohibitAppendWritesInBackingIndices(DocWriteRequest<?> writeRequest, IndexAbstraction indexAbstraction) {
        DocWriteRequest.OpType opType = writeRequest.opType();
        if ((opType == OpType.CREATE || opType == OpType.INDEX) == false) {
            // op type not create or index, then bail early
            return;
        }
        if (indexAbstraction == null) {
            return;
        }
        if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            return;
        }
        if (indexAbstraction.getParentDataStream() == null) {
            return;
        }

        DataStream dataStream = indexAbstraction.getParentDataStream();

        // At this point with write op is targeting a backing index of a data stream directly,
        // so checking if write op is append-only and if so fail.
        // (Updates and deletes are allowed to target a backing index)

        // CREATE op_type is considered append-only and
        // INDEX op_type is considered append-only when no if_primary_term and if_seq_no is specified.
        // (the latter maybe an update, but at this stage we can't determine that. In order to determine
        // that an engine level change is needed and for now this check is sufficient.)
        if (opType == DocWriteRequest.OpType.CREATE) {
            throw new IllegalArgumentException(
                "index request with op_type=create targeting backing indices is disallowed, "
                    + "target corresponding data stream ["
                    + dataStream.getName()
                    + "] instead"
            );
        }
        if (writeRequest.ifPrimaryTerm() == UNASSIGNED_PRIMARY_TERM && writeRequest.ifSeqNo() == UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException(
                "index request with op_type=index and no if_primary_term and if_seq_no set "
                    + "targeting backing indices is disallowed, target corresponding data stream ["
                    + dataStream.getName()
                    + "] instead"
            );
        }
    }

    static void prohibitCustomRoutingOnDataStream(DocWriteRequest<?> writeRequest, IndexAbstraction indexAbstraction) {
        if (indexAbstraction == null) {
            return;
        }
        if (indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM) {
            return;
        }

        if (writeRequest.routing() != null) {
            DataStream dataStream = (DataStream) indexAbstraction;
            if (dataStream.isAllowCustomRouting() == false) {
                throw new IllegalArgumentException(
                    "index request targeting data stream ["
                        + dataStream.getName()
                        + "] specifies a custom routing but the [allow_custom_routing] setting was "
                        + "not enabled in the data stream's template."
                );
            }
        }
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

    void createIndex(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener) {
        client.execute(AutoCreateAction.INSTANCE, createIndexRequest, listener);
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

    void executeBulk(
        Task task,
        BulkRequest bulkRequest,
        long startTimeNanos,
        ActionListener<BulkResponse> listener,
        Executor executor,
        AtomicArray<BulkItemResponse> responses,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated
    ) {
        new BulkOperation(
            task,
            threadPool,
            executor,
            clusterService,
            bulkRequest,
            client,
            responses,
            indicesThatCannotBeCreated,
            indexNameExpressionResolver,
            relativeTimeNanosProvider,
            startTimeNanos,
            listener,
            failureStoreMetrics
        ).run();
    }

    /**
     * See {@link #resolveFailureStore(String, Metadata, long)}
     */
    // Visibility for testing
    static Boolean resolveFailureInternal(String indexName, Metadata metadata, long epochMillis) {
        if (DataStream.isFailureStoreFeatureFlagEnabled() == false) {
            return null;
        }
        var resolution = resolveFailureStoreFromMetadata(indexName, metadata, epochMillis);
        if (resolution != null) {
            return resolution;
        }
        return resolveFailureStoreFromTemplate(indexName, metadata);
    }

    @Override
    protected Boolean resolveFailureStore(String indexName, Metadata metadata, long time) {
        return resolveFailureInternal(indexName, metadata, time);
    }

    /**
     * Determines if an index name is associated with an existing data stream that has a failure store enabled.
     * @param indexName The index name to check.
     * @param metadata Cluster state metadata.
     * @param epochMillis A timestamp to use when resolving date math in the index name.
     * @return true if the given index name corresponds to an existing data stream with a failure store enabled.
     */
    private static Boolean resolveFailureStoreFromMetadata(String indexName, Metadata metadata, long epochMillis) {
        if (indexName == null) {
            return null;
        }

        // Get index abstraction, resolving date math if it exists
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup()
            .get(IndexNameExpressionResolver.resolveDateMathExpression(indexName, epochMillis));
        if (indexAbstraction == null || indexAbstraction.isDataStreamRelated() == false) {
            return null;
        }

        // We only store failures if the failure is being written to a data stream,
        // not when directly writing to backing indices/failure stores
        DataStream targetDataStream = DataStream.resolveDataStream(indexAbstraction, metadata);

        // We will store the failure if the write target belongs to a data stream with a failure store.
        return targetDataStream != null && targetDataStream.isFailureStoreEnabled();
    }

    /**
     * Determines if an index name is associated with an index template that has a data stream failure store enabled.
     * @param indexName The index name to check.
     * @param metadata Cluster state metadata.
     * @return true if the given index name corresponds to an index template with a data stream failure store enabled.
     */
    private static Boolean resolveFailureStoreFromTemplate(String indexName, Metadata metadata) {
        if (indexName == null) {
            return null;
        }

        // Check to see if the index name matches any templates such that an index would have been attributed
        // We don't check v1 templates at all because failure stores can only exist on data streams via a v2 template
        String template = MetadataIndexTemplateService.findV2Template(metadata, indexName, false);
        if (template != null) {
            // Check if this is a data stream template or if it is just a normal index.
            ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(template);
            if (composableIndexTemplate.getDataStreamTemplate() != null) {
                // Check if the data stream has the failure store enabled
                return composableIndexTemplate.getDataStreamTemplate().hasFailureStore();
            }
        }

        // Could not locate a failure store via template
        return null;
    }
}
