/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.ProjectStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.action.bulk.TransportBulkAction.unwrappingSingleItemBulkResponse;
import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.EXCLUDED_DATA_STREAMS_KEY;

public class TransportUpdateAction extends HandledTransportAction<UpdateRequest, UpdateResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final String shardActionName;

    public static final String NAME = "indices:data/write/update";
    public static final ActionType<UpdateResponse> TYPE = new ActionType<>(NAME);
    private final AutoCreateIndex autoCreateIndex;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;
    private final NodeClient client;

    @Inject
    public TransportUpdateAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        AutoCreateIndex autoCreateIndex,
        NodeClient client
    ) {
        this(
            NAME,
            threadPool,
            clusterService,
            projectResolver,
            transportService,
            updateHelper,
            actionFilters,
            indexNameExpressionResolver,
            indicesService,
            autoCreateIndex,
            client
        );
    }

    // visible for test
    @SuppressWarnings("this-escape")
    protected TransportUpdateAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        AutoCreateIndex autoCreateIndex,
        NodeClient client
    ) {
        super(actionName, transportService, actionFilters, UpdateRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.shardActionName = actionName + "[s]";
        transportService.registerRequestHandler(
            shardActionName,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            UpdateRequest::new,
            this::handleShardRequest
        );

        this.updateHelper = updateHelper;
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
        this.client = client;
    }

    private ProjectState getProjectState() {
        return projectResolver.getProjectState(clusterService.state());
    }

    protected Executor executor(ShardId shardId) {
        return executor(indicesService.indexServiceSafe(shardId.getIndex()));
    }

    private Executor executor(IndexService indexService) {
        return threadPool.executor(indexService.getIndexSettings().getIndexMetadata().isSystem() ? Names.SYSTEM_WRITE : Names.WRITE);
    }

    protected UpdateResponse newResponse(StreamInput in) throws IOException {
        return new UpdateResponse(in);
    }

    private static ClusterBlockException checkGlobalBlock(ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    private ClusterBlockException checkRequestBlock(ProjectState state, UpdateRequest request) {
        return state.blocks().indexBlockedException(state.projectId(), ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    private boolean retryOnFailure(Exception e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    /**
     * Resolves the request. Throws an exception if the request cannot be resolved.
     */
    protected void resolveRequest(ProjectState state, UpdateRequest docWriteRequest) {
        docWriteRequest.routing(state.metadata().resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));
        requireSliceRoutingWhenEnabled(state, docWriteRequest);
    }

    private static void requireSliceRoutingWhenEnabled(ProjectState state, UpdateRequest request) {
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled() == false) {
            return;
        }
        final String concreteName = IndexNameExpressionResolver.resolveDateMathExpression(request.index());
        final boolean sliceEnabled = Optional.ofNullable(state.metadata().getIndicesLookup().get(concreteName))
            .map(indexAbstraction -> indexAbstraction.getWriteIndex())
            .map(state.metadata()::index)
            .map(metadata -> IndexSettings.SLICE_ENABLED.get(metadata.getSettings()))
            .orElse(false);
        SliceIndexing.validateSliceRoutingRequirement(
            sliceEnabled,
            request.isRoutingFromSlice(),
            request.routing(),
            "update request",
            request.index()
        );
    }

    @Override
    protected void doExecute(Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        final ProjectState state = getProjectState();
        if (request.isRequireAlias() && (state.metadata().hasAlias(request.index()) == false)) {
            throw new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + request.index() + "] is not an alias",
                request.index()
            );
        }
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), state.metadata())) {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest().index(request.index()).cause("auto(update api)").masterNodeTimeout(request.timeout()),
                    new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            innerExecute(request, listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                                // we have the index, do it
                                try {
                                    innerExecute(request, listener);
                                } catch (Exception inner) {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    }
                );
        } else {
            innerExecute(request, listener);
        }
    }

    private void innerExecute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected ShardIterator shards(ProjectState projectState, UpdateRequest request) {
        if (request.getShardId() != null) {
            return projectState.routingTable().index(request.concreteIndex()).shard(request.getShardId().getId()).primaryShardIt();
        }
        IndexMetadata indexMetadata = projectState.metadata().index(request.concreteIndex());
        if (indexMetadata == null) {
            throw new IndexNotFoundException(request.concreteIndex());
        }
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        int shardId = indexRouting.updateShard(request.id(), request.routing());
        return RoutingTable.shardRoutingTable(projectState.routingTable().index(request.concreteIndex()), shardId).primaryShardIt();
    }

    private void handleShardRequest(UpdateRequest request, TransportChannel channel, Task task) {
        executor(request.shardId).execute(
            ActionRunnable.wrap(new ChannelActionListener<UpdateResponse>(channel), l -> shardOperation(request, l))
        );
    }

    protected static TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        try {
            shardOperation(request, listener, 0);
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount)
        throws IOException {
        final ShardId shardId = request.getShardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final MappingLookup mappingLookup = indexShard.mapperService().mappingLookup();

        var executor = executor(indexService);
        assert ThreadPool.assertCurrentThreadPool(Names.SYSTEM_WRITE, Names.WRITE);

        SubscribableListener.<Void>newForked((l) -> indexShard.ensureMutable(l, false, EsExecutors.DIRECT_EXECUTOR_SERVICE))
        // Make sure to fork back to a `write` thread pool if necessary
        .<UpdateHelper.Result>andThen(executor, threadPool.getThreadContext(), (l, unused) -> ActionListener.completeWith(l, () -> {
            assert ThreadPool.assertCurrentThreadPool(Names.SYSTEM_WRITE, Names.WRITE);
            return deleteInferenceResults(
                request,
                // Gets the doc using the engine
                updateHelper.prepare(
                    request,
                    indexShard,
                    threadPool::absoluteTimeInMillis,
                    // Exclude inference fields to ensure embeddings are recomputed.
                    FetchSourceContext.FETCH_ALL_SOURCE_EXCLUDE_INFERENCE_FIELDS,
                    SplitShardCountSummary.UNSET
                ),
                indexService.getMetadata(),
                mappingLookup
            );
        }))
            // Proceed with a single item bulk request
            .<UpdateResponse>andThen((l, result) -> {
                switch (result.getResponseResult()) {
                    case CREATED -> {
                        IndexRequest upsertRequest = result.action();
                        // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                        final BytesReference upsertSourceBytes = upsertRequest.source();
                        client.bulk(
                            toSingleItemBulkRequest(upsertRequest),
                            unwrappingSingleItemBulkResponse(ActionListener.<DocWriteResponse>wrap(response -> {
                                UpdateResponse update = new UpdateResponse(
                                    response.getShardInfo(),
                                    response.getShardId(),
                                    response.getId(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    response.getVersion(),
                                    response.getResult()
                                );
                                if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
                                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                                        upsertSourceBytes,
                                        true,
                                        upsertRequest.getContentType()
                                    );
                                    update.setGetResult(
                                        UpdateHelper.extractGetResult(
                                            request,
                                            request.concreteIndex(),
                                            mappingLookup,
                                            response.getSeqNo(),
                                            response.getPrimaryTerm(),
                                            response.getVersion(),
                                            sourceAndContent.v2(),
                                            sourceAndContent.v1(),
                                            upsertSourceBytes
                                        )
                                    );
                                } else {
                                    update.setGetResult(null);
                                }
                                update.setForcedRefresh(response.forcedRefresh());
                                l.onResponse(update);
                            }, exception -> handleUpdateFailureWithRetry(l, request, exception, retryCount)))
                        );
                    }
                    case UPDATED -> {
                        IndexRequest indexRequest = result.action();
                        // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                        final BytesReference indexSourceBytes = indexRequest.source();
                        client.bulk(
                            toSingleItemBulkRequest(indexRequest),
                            unwrappingSingleItemBulkResponse(ActionListener.<DocWriteResponse>wrap(response -> {
                                UpdateResponse update = new UpdateResponse(
                                    response.getShardInfo(),
                                    response.getShardId(),
                                    response.getId(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    response.getVersion(),
                                    response.getResult()
                                );
                                update.setGetResult(
                                    UpdateHelper.extractGetResult(
                                        request,
                                        request.concreteIndex(),
                                        mappingLookup,
                                        response.getSeqNo(),
                                        response.getPrimaryTerm(),
                                        response.getVersion(),
                                        result.updatedSourceAsMap(),
                                        result.updateSourceContentType(),
                                        indexSourceBytes
                                    )
                                );
                                update.setForcedRefresh(response.forcedRefresh());
                                l.onResponse(update);
                            }, exception -> handleUpdateFailureWithRetry(l, request, exception, retryCount)))
                        );
                    }
                    case DELETED -> {
                        DeleteRequest deleteRequest = result.action();
                        client.bulk(
                            toSingleItemBulkRequest(deleteRequest),
                            unwrappingSingleItemBulkResponse(ActionListener.<DeleteResponse>wrap(response -> {
                                UpdateResponse update = new UpdateResponse(
                                    response.getShardInfo(),
                                    response.getShardId(),
                                    response.getId(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    response.getVersion(),
                                    response.getResult()
                                );
                                update.setGetResult(
                                    UpdateHelper.extractGetResult(
                                        request,
                                        request.concreteIndex(),
                                        mappingLookup,
                                        response.getSeqNo(),
                                        response.getPrimaryTerm(),
                                        response.getVersion(),
                                        result.updatedSourceAsMap(),
                                        result.updateSourceContentType(),
                                        null
                                    )
                                );
                                update.setForcedRefresh(response.forcedRefresh());
                                l.onResponse(update);
                            }, exception -> handleUpdateFailureWithRetry(l, request, exception, retryCount)))
                        );
                    }
                    case NOOP -> {
                        UpdateResponse update = result.action();
                        IndexService indexServiceOrNull = indicesService.indexService(shardId.getIndex());
                        if (indexServiceOrNull != null) {
                            IndexShard shard = indexService.getShardOrNull(shardId.getId());
                            if (shard != null) {
                                shard.noopUpdate();
                            }
                        }
                        l.onResponse(update);
                    }
                    default -> throw new IllegalStateException("Illegal result " + result.getResponseResult());
                }
            })
            .addListener(listener);
    }

    private void handleUpdateFailureWithRetry(
        final ActionListener<UpdateResponse> listener,
        final UpdateRequest request,
        final Exception failure,
        int retryCount
    ) {
        final Throwable cause = unwrapCause(failure);
        if (cause instanceof VersionConflictEngineException versionConflictEngineException && retryCount < request.retryOnConflict()) {
            logger.trace(
                "Retry attempt [{}] of [{}] on version conflict on [{}][{}][{}]",
                retryCount + 1,
                request.retryOnConflict(),
                request.index(),
                request.getShardId(),
                request.id()
            );

            final Executor executor;
            try {
                executor = executor(request.getShardId());
            } catch (Exception e) {
                // might fail if shard no longer exists locally, in which case we cannot retry
                e.addSuppressed(versionConflictEngineException);
                listener.onFailure(e);
                return;
            }
            executor.execute(ActionRunnable.wrap(listener, l -> shardOperation(request, l, retryCount + 1)));
            return;
        }
        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
    }

    /**
     * <p>
     * Delete stale inference results from the provided {@link UpdateHelper.Result} instance.
     * </p>
     * <p>
     * We need to do this because when handling Bulk API requests (which the Update API generates), we assume any inference results present
     * in source are up-to-date.
     * We do this to support reindex and update by query use cases without re-generating inference results unnecessarily.
     * </p>
     *
     * @param updateRequest The update request
     * @param result The result generated using the update request
     * @param indexMetadata The index metadata
     * @param mappingLookup The index's mapping lookup
     * @return A result with stale inference results removed from source
     */
    private static UpdateHelper.Result deleteInferenceResults(
        UpdateRequest updateRequest,
        UpdateHelper.Result result,
        IndexMetadata indexMetadata,
        MappingLookup mappingLookup
    ) {
        if (result.getResponseResult() != DocWriteResponse.Result.UPDATED || InferenceMetadataFieldsMapper.isEnabled(mappingLookup)) {
            return result;
        }

        Map<String, InferenceFieldMetadata> inferenceFields = indexMetadata.getInferenceFields();
        if (inferenceFields.isEmpty()) {
            return result;
        }

        if (updateRequest.script() != null) {
            throw new ElasticsearchStatusException(
                "Cannot apply update with a script on indices that contain inference field(s)",
                RestStatus.BAD_REQUEST
            );
        }

        IndexRequest doc = updateRequest.doc();
        if (doc == null) {
            // No doc update, nothing to do
            return result;
        }

        Map<String, Object> updateRequestSource = doc.sourceAsMap();
        Map<String, Object> updatedSource = result.updatedSourceAsMap();
        boolean updatedSourceModified = false;
        for (var entry : inferenceFields.entrySet()) {
            String inferenceFieldName = entry.getKey();
            Mapper mapper = mappingLookup.getMapper(inferenceFieldName);

            if (mapper instanceof InferenceFieldMapper) {
                String[] sourceFields = entry.getValue().getSourceFields();
                for (String sourceField : sourceFields) {
                    if (sourceField.equals(inferenceFieldName) == false
                        && XContentMapValues.extractValue(sourceField, updateRequestSource) != null) {
                        // Replace the inference field's value with its original value (i.e. the user-specified value).
                        // This has two important side effects:
                        // - The inference field value will remain parsable by its mapper
                        // - The inference results will be removed, forcing them to be re-generated downstream
                        updatedSource.put(inferenceFieldName, getOriginalValueLegacy(inferenceFieldName, updatedSource));
                        updatedSourceModified = true;
                        break;
                    }
                }
            } else {
                throw new IllegalStateException(
                    "Field [" + inferenceFieldName + "] is of type [ " + mapper.typeName() + "], which is not an inference field"
                );
            }
        }

        UpdateHelper.Result returnedResult = result;
        if (updatedSourceModified) {
            XContentType contentType = result.updateSourceContentType();
            IndexRequest indexRequest = result.action();
            indexRequest.source(updatedSource, contentType);

            returnedResult = new UpdateHelper.Result(indexRequest, result.getResponseResult(), updatedSource, contentType);
        }

        return returnedResult;
    }

    /**
     * Get the field's original value (i.e. the value the user specified) from the provided source.
     *
     * @param sourceAsMap The source as a map
     * @return The field's original value, or {@code null} if none was provided
     */
    private static Object getOriginalValueLegacy(String fullPath, Map<String, Object> sourceAsMap) {
        // TODO: Fix bug here when semantic text field is in an object
        Object fieldValue = sourceAsMap.get(fullPath);
        if (fieldValue == null) {
            return null;
        } else if (fieldValue instanceof Map<?, ?> == false) {
            // Don't try to further validate the non-map value, that will be handled when the source is fully parsed
            return fieldValue;
        }

        Map<String, Object> fieldValueMap = XContentMapValues.nodeMapValue(fieldValue, "Field [" + fullPath + "]");
        return XContentMapValues.extractValue("text", fieldValueMap);
    }

    class AsyncSingleAction {

        private final ActionListener<UpdateResponse> listener;
        private final UpdateRequest request;
        private volatile ProjectStateObserver observer;
        private ShardIterator shardIt;

        AsyncSingleAction(UpdateRequest request, ActionListener<UpdateResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            final ProjectState state = getProjectState();
            this.observer = new ProjectStateObserver(state, clusterService, request.timeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        protected void doStart(ProjectState projectState) {
            try {
                ClusterBlockException blockException = checkGlobalBlock(projectState);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                try {
                    request.concreteIndex(indexNameExpressionResolver.concreteWriteIndex(projectState.metadata(), request).getName());
                } catch (IndexNotFoundException e) {
                    if (request.includeDataStreams() == false && e.getMetadataKeys().contains(EXCLUDED_DATA_STREAMS_KEY)) {
                        throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                    } else {
                        throw e;
                    }
                }
                resolveRequest(projectState, request);
                blockException = checkRequestBlock(projectState, request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(projectState, request);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(null);
                return;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (shard.active() == false) {
                retry(null);
                return;
            }

            request.shardId = shardIt.shardId();
            DiscoveryNode node = projectState.cluster().nodes().get(shard.currentNodeId());
            transportService.sendRequest(
                node,
                shardActionName,
                request,
                transportOptions(),
                new ActionListenerResponseHandler<>(
                    listener,
                    TransportUpdateAction.this::newResponse,
                    TransportResponseHandler.TRANSPORT_WORKER
                ) {
                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = exp.unwrapCause();
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException || retryOnFailure(exp)) {
                            retry((Exception) cause);
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                }
            );
        }

        void retry(@Nullable final Exception failure) {
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                Exception listenFailure = failure;
                if (listenFailure == null) {
                    if (shardIt == null) {
                        listenFailure = new UnavailableShardsException(
                            request.concreteIndex(),
                            -1,
                            "Timeout waiting for [{}], request: {}",
                            request.timeout(),
                            actionName
                        );
                    } else {
                        listenFailure = new UnavailableShardsException(
                            shardIt.shardId(),
                            "[{}] shardIt, [{}] active : Timeout waiting for [{}], request: {}",
                            shardIt.size(),
                            shardIt.sizeActive(),
                            request.timeout(),
                            actionName
                        );
                    }
                }
                listener.onFailure(listenFailure);
                return;
            }

            observer.waitForNextChange(new ProjectStateObserver.Listener() {
                @Override
                public void onProjectStateChange(ProjectState projectState) {
                    doStart(projectState);
                }

                @Override
                public void onProjectMissing(ProjectId projectId, ClusterState clusterState) {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            "project ["
                                + projectId
                                + "] does not exist in cluster state ["
                                + clusterState.stateUUID()
                                + "] version ["
                                + clusterState.version()
                                + "]"
                        )
                    );
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // just to be on the safe side, see if we can start it now?
                    observer.observeLastAppliedState(this);
                }
            }, request.timeout());
        }
    }
}
