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
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.EXCLUDED_DATA_STREAMS_KEY;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Groups bulk request items by shard, optionally creating non-existent indices and
 * delegates to {@link TransportShardBulkAction} for shard-level bulk execution
 */
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private static final Logger logger = LogManager.getLogger(TransportBulkAction.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IngestService ingestService;
    private final LongSupplier relativeTimeProvider;
    private final IngestActionForwarder ingestForwarder;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private static final String DROPPED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";
    private final IndexingPressure indexingPressure;
    private final SystemIndices systemIndices;

    @Inject
    public TransportBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
    ) {
        this(
            threadPool,
            transportService,
            clusterService,
            ingestService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            indexingPressure,
            systemIndices,
            System::nanoTime
        );
    }

    public TransportBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        LongSupplier relativeTimeProvider
    ) {
        super(BulkAction.NAME, transportService, actionFilters, BulkRequest::new, ThreadPool.Names.SAME);
        Objects.requireNonNull(relativeTimeProvider);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.relativeTimeProvider = relativeTimeProvider;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexingPressure = indexingPressure;
        this.systemIndices = systemIndices;
        clusterService.addStateApplier(this.ingestForwarder);
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
        return ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected exactly one item in bulk response";
            final BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                @SuppressWarnings("unchecked")
                final Response response = (Response) bulkItemResponse.getResponse();
                listener.onResponse(response);
            } else {
                listener.onFailure(bulkItemResponse.getFailure().getCause());
            }
        }, listener::onFailure);
    }

    @Override
    protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> outerListener) {
        // As a work-around to support `?refresh`, explicitly replace the refresh policy with a call to the Refresh API.
        // TODO: Replace with a less hacky approach.
        ActionListener<BulkResponse> listener = outerListener;
        if (DiscoveryNode.isStateless(clusterService.getSettings()) && bulkRequest.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE) {
            listener = outerListener.delegateFailure((l, r) -> { client.admin().indices().prepareRefresh().execute(l.map(ignored -> r)); });
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        }
        /*
         * This is called on the Transport tread so we can check the indexing
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
        final boolean isOnlySystem = isOnlySystem(bulkRequest, clusterService.state().metadata().getIndicesLookup(), systemIndices);
        final Releasable releasable = indexingPressure.markCoordinatingOperationStarted(indexingOps, indexingBytes, isOnlySystem);
        final ActionListener<BulkResponse> releasingListener = ActionListener.runBefore(listener, releasable::close);
        final String executorName = isOnlySystem ? Names.SYSTEM_WRITE : Names.WRITE;
        threadPool.executor(Names.WRITE).execute(new ActionRunnable<>(releasingListener) {
            @Override
            protected void doRun() {
                doInternalExecute(task, bulkRequest, executorName, releasingListener);
            }
        });
    }

    protected void doInternalExecute(Task task, BulkRequest bulkRequest, String executorName, ActionListener<BulkResponse> listener) {
        final long startTime = relativeTime();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

        boolean hasIndexRequestsWithPipelines = false;
        final Metadata metadata = clusterService.state().getMetadata();
        final Version minNodeVersion = clusterService.state().getNodes().getMinNodeVersion();
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests) {
            IndexRequest indexRequest = getIndexWriteRequest(actionRequest);
            if (indexRequest != null) {
                // Each index request needs to be evaluated, because this method also modifies the IndexRequest
                boolean indexRequestHasPipeline = IngestService.resolvePipelines(actionRequest, indexRequest, metadata);
                hasIndexRequestsWithPipelines |= indexRequestHasPipeline;
            }

            if (actionRequest instanceof IndexRequest ir) {
                ir.checkAutoIdWithOpTypeCreateSupportedByVersion(minNodeVersion.transportVersion);
                if (ir.getAutoGeneratedTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
                    throw new IllegalArgumentException("autoGeneratedTimestamp should not be set externally");
                }
            }
        }

        if (hasIndexRequestsWithPipelines) {
            // this method (doExecute) will be called again, but with the bulk requests updated from the ingest node processing but
            // also with IngestService.NOOP_PIPELINE_NAME on each request. This ensures that this on the second time through this method,
            // this path is never taken.
            try {
                if (Assertions.ENABLED) {
                    final boolean arePipelinesResolved = bulkRequest.requests()
                        .stream()
                        .map(TransportBulkAction::getIndexWriteRequest)
                        .filter(Objects::nonNull)
                        .allMatch(IndexRequest::isPipelineResolved);
                    assert arePipelinesResolved : bulkRequest;
                }
                if (clusterService.localNode().isIngestNode()) {
                    processBulkIndexIngestRequest(task, bulkRequest, executorName, listener);
                } else {
                    ingestForwarder.forwardIngestRequest(BulkAction.INSTANCE, bulkRequest, listener);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }

        // Attempt to create all the indices that we're going to need during the bulk before we start.
        // Step 1: collect all the indices in the request
        final Map<String, Boolean> indices = bulkRequest.requests.stream()
            // delete requests should not attempt to create the index (if the index does not
            // exists), unless an external versioning is used
            .filter(
                request -> request.opType() != DocWriteRequest.OpType.DELETE
                    || request.versionType() == VersionType.EXTERNAL
                    || request.versionType() == VersionType.EXTERNAL_GTE
            )
            .collect(Collectors.toMap(DocWriteRequest::index, DocWriteRequest::isRequireAlias, (v1, v2) -> v1 || v2));

        // Step 2: filter the list of indices to find those that don't currently exist.
        final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();
        Set<String> autoCreateIndices = new HashSet<>();
        ClusterState state = clusterService.state();
        for (Map.Entry<String, Boolean> indexAndFlag : indices.entrySet()) {
            final String index = indexAndFlag.getKey();
            boolean shouldAutoCreate = indexNameExpressionResolver.hasIndexAbstraction(index, state) == false;
            // We should only auto create if we are not requiring it to be an alias
            if (shouldAutoCreate && (indexAndFlag.getValue() == false)) {
                autoCreateIndices.add(index);
            }
        }

        // Step 3: create all the indices that are missing, if there are any missing. start the bulk after all the creates come back.
        if (autoCreateIndices.isEmpty()) {
            executeBulk(task, bulkRequest, startTime, listener, executorName, responses, indicesThatCannotBeCreated);
        } else {
            final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
            for (String index : autoCreateIndices) {
                createIndex(index, bulkRequest.timeout(), new ActionListener<>() {
                    @Override
                    public void onResponse(CreateIndexResponse result) {
                        if (counter.decrementAndGet() == 0) {
                            threadPool.executor(executorName).execute(new ActionRunnable<>(listener) {
                                @Override
                                protected void doRun() {
                                    executeBulk(
                                        task,
                                        bulkRequest,
                                        startTime,
                                        listener,
                                        executorName,
                                        responses,
                                        indicesThatCannotBeCreated
                                    );
                                }
                            });
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof IndexNotFoundException) {
                            indicesThatCannotBeCreated.put(index, (IndexNotFoundException) cause);
                        } else if ((cause instanceof ResourceAlreadyExistsException) == false) {
                            // fail all requests involving this index, if create didn't work
                            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                DocWriteRequest<?> request = bulkRequest.requests.get(i);
                                if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                    bulkRequest.requests.set(i, null);
                                }
                            }
                        }
                        if (counter.decrementAndGet() == 0) {
                            final ActionListener<BulkResponse> wrappedListener = ActionListener.wrap(listener::onResponse, inner -> {
                                inner.addSuppressed(e);
                                listener.onFailure(inner);
                            });
                            threadPool.executor(executorName).execute(new ActionRunnable<>(wrappedListener) {
                                @Override
                                protected void doRun() {
                                    executeBulk(
                                        task,
                                        bulkRequest,
                                        startTime,
                                        wrappedListener,
                                        executorName,
                                        responses,
                                        indicesThatCannotBeCreated
                                    );
                                }

                                @Override
                                public void onRejection(Exception rejectedException) {
                                    rejectedException.addSuppressed(e);
                                    super.onRejection(rejectedException);
                                }
                            });
                        }
                    }
                });
            }
        }
    }

    static void prohibitAppendWritesInBackingIndices(DocWriteRequest<?> writeRequest, Metadata metadata) {
        DocWriteRequest.OpType opType = writeRequest.opType();
        if ((opType == OpType.CREATE || opType == OpType.INDEX) == false) {
            // op type not create or index, then bail early
            return;
        }
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(writeRequest.index());
        if (indexAbstraction == null) {
            return;
        }
        if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            return;
        }
        if (indexAbstraction.getParentDataStream() == null) {
            return;
        }

        DataStream dataStream = indexAbstraction.getParentDataStream().getDataStream();

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
        if (opType == DocWriteRequest.OpType.INDEX
            && writeRequest.ifPrimaryTerm() == UNASSIGNED_PRIMARY_TERM
            && writeRequest.ifSeqNo() == UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException(
                "index request with op_type=index and no if_primary_term and if_seq_no set "
                    + "targeting backing indices is disallowed, target corresponding data stream ["
                    + dataStream.getName()
                    + "] instead"
            );
        }
    }

    static void prohibitCustomRoutingOnDataStream(DocWriteRequest<?> writeRequest, Metadata metadata) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(writeRequest.index());
        if (indexAbstraction == null) {
            return;
        }
        if (indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM) {
            return;
        }

        if (writeRequest.routing() != null) {
            IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) indexAbstraction;
            if (dataStream.getDataStream().isAllowCustomRouting() == false) {
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

    void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index(index);
        createIndexRequest.cause("auto(bulk api)");
        createIndexRequest.masterNodeTimeout(timeout);
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

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    /**
     * retries on retryable cluster blocks, resolves item requests,
     * constructs shard bulk requests and delegates execution to shard bulk action
     * */
    private final class BulkOperation extends ActionRunnable<BulkResponse> {
        private final Task task;
        private BulkRequest bulkRequest; // set to null once all requests are sent out
        private final ActionListener<BulkResponse> listener;
        private final AtomicArray<BulkItemResponse> responses;
        private final long startTimeNanos;
        private final ClusterStateObserver observer;
        private final Map<String, IndexNotFoundException> indicesThatCannotBeCreated;
        private final String executorName;

        BulkOperation(
            Task task,
            BulkRequest bulkRequest,
            ActionListener<BulkResponse> listener,
            String executorName,
            AtomicArray<BulkItemResponse> responses,
            long startTimeNanos,
            Map<String, IndexNotFoundException> indicesThatCannotBeCreated
        ) {
            super(listener);
            this.task = task;
            this.bulkRequest = bulkRequest;
            this.listener = listener;
            this.responses = responses;
            this.startTimeNanos = startTimeNanos;
            this.indicesThatCannotBeCreated = indicesThatCannotBeCreated;
            this.executorName = executorName;
            this.observer = new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        protected void doRun() {
            assert bulkRequest != null;
            final ClusterState clusterState = observer.setAndGetObservedState();
            if (handleBlockExceptions(clusterState)) {
                return;
            }
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
                        ia.getName().equals(docWriteRequest.index()) == false
                        && docWriteRequest.opType() != DocWriteRequest.OpType.CREATE) {
                        throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                    }

                    prohibitCustomRoutingOnDataStream(docWriteRequest, metadata);
                    prohibitAppendWritesInBackingIndices(docWriteRequest, metadata);
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

            if (requestsByShard.isEmpty()) {
                listener.onResponse(
                    new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos))
                );
                return;
            }

            final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
            String nodeId = clusterService.localNode().getId();
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();
                BulkShardRequest bulkShardRequest = new BulkShardRequest(
                    shardId,
                    bulkRequest.getRefreshPolicy(),
                    requests.toArray(new BulkItemRequest[requests.size()])
                );
                bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(bulkRequest.timeout());
                bulkShardRequest.routedBasedOnClusterVersion(clusterState.version());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }
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
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // create failures for all relevant requests
                        for (BulkItemRequest request : requests) {
                            final String indexName = request.index();
                            DocWriteRequest<?> docWriteRequest = request.request();
                            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexName, docWriteRequest.id(), e);
                            responses.set(request.id(), BulkItemResponse.failure(request.id(), docWriteRequest.opType(), failure));
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    private void finishHim() {
                        listener.onResponse(
                            new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos))
                        );
                    }
                });
            }
            bulkRequest = null; // allow memory for bulk request items to be reclaimed before all items have been completed
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
    }

    void executeBulk(
        Task task,
        BulkRequest bulkRequest,
        long startTimeNanos,
        ActionListener<BulkResponse> listener,
        String executorName,
        AtomicArray<BulkItemResponse> responses,
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated
    ) {
        new BulkOperation(task, bulkRequest, listener, executorName, responses, startTimeNanos, indicesThatCannotBeCreated).run();
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
                return indexAbstractions.computeIfAbsent(
                    request.index(),
                    key -> indexNameExpressionResolver.resolveWriteIndexAbstraction(state, request)
                );
            } catch (IndexNotFoundException e) {
                if (e.getMetadataKeys().contains(EXCLUDED_DATA_STREAMS_KEY)) {
                    throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams", e);
                } else {
                    throw e;
                }
            }
        }

        IndexRouting routing(Index index) {
            return routings.computeIfAbsent(index, idx -> IndexRouting.fromIndexMetadata(state.metadata().getIndexSafe(idx)));
        }
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }

    private void processBulkIndexIngestRequest(
        Task task,
        BulkRequest original,
        String executorName,
        ActionListener<BulkResponse> listener
    ) {
        final long ingestStartTimeInNanos = System.nanoTime();
        final BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        ingestService.executeBulkRequest(
            original.numberOfActions(),
            () -> bulkRequestModifier,
            bulkRequestModifier::markItemAsDropped,
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
                                doInternalExecute(task, bulkRequest, executorName, actionListener);
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
                            threadPool.executor(Names.SAME).execute(runnable);
                        } else {
                            threadPool.executor(executorName).execute(runnable);
                        }
                    }
                }
            },
            executorName
        );
    }

    static final class BulkRequestModifier implements Iterator<DocWriteRequest<?>> {

        final BulkRequest bulkRequest;
        final SparseFixedBitSet failedSlots;
        final List<BulkItemResponse> itemResponses;
        final AtomicIntegerArray originalSlots;

        volatile int currentSlot = -1;

        BulkRequestModifier(BulkRequest bulkRequest) {
            this.bulkRequest = bulkRequest;
            this.failedSlots = new SparseFixedBitSet(bulkRequest.requests().size());
            this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
            this.originalSlots = new AtomicIntegerArray(bulkRequest.requests().size()); // oversize, but that's ok
        }

        @Override
        public DocWriteRequest<?> next() {
            return bulkRequest.requests().get(++currentSlot);
        }

        @Override
        public boolean hasNext() {
            return (currentSlot + 1) < bulkRequest.requests().size();
        }

        BulkRequest getBulkRequest() {
            if (itemResponses.isEmpty()) {
                return bulkRequest;
            } else {
                BulkRequest modifiedBulkRequest = new BulkRequest();
                modifiedBulkRequest.setRefreshPolicy(bulkRequest.getRefreshPolicy());
                modifiedBulkRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                modifiedBulkRequest.timeout(bulkRequest.timeout());

                int slot = 0;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                for (int i = 0; i < requests.size(); i++) {
                    DocWriteRequest<?> request = requests.get(i);
                    if (failedSlots.get(i) == false) {
                        modifiedBulkRequest.add(request);
                        originalSlots.set(slot++, i);
                    }
                }
                return modifiedBulkRequest;
            }
        }

        ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return actionListener.map(
                    response -> new BulkResponse(response.getItems(), response.getTook().getMillis(), ingestTookInMillis)
                );
            } else {
                return actionListener.map(response -> {
                    BulkItemResponse[] items = response.getItems();
                    for (int i = 0; i < items.length; i++) {
                        itemResponses.add(originalSlots.get(i), response.getItems()[i]);
                    }
                    return new BulkResponse(
                        itemResponses.toArray(new BulkItemResponse[0]),
                        response.getTook().getMillis(),
                        ingestTookInMillis
                    );
                });
            }
        }

        synchronized void markItemAsDropped(int slot) {
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            failedSlots.set(slot);
            final String id = indexRequest.id() == null ? DROPPED_ITEM_WITH_AUTO_GENERATED_ID : indexRequest.id();
            itemResponses.add(
                BulkItemResponse.success(
                    slot,
                    indexRequest.opType(),
                    new UpdateResponse(
                        new ShardId(indexRequest.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                        id,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        indexRequest.version(),
                        DocWriteResponse.Result.NOOP
                    )
                )
            );
        }

        synchronized void markItemAsFailed(int slot, Exception e) {
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            // We hit a error during preprocessing a request, so we:
            // 1) Remember the request item slot from the bulk, so that we're done processing all requests we know what failed
            // 2) Add a bulk item failure for this request
            // 3) Continue with the next request in the bulk.
            failedSlots.set(slot);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexRequest.index(), indexRequest.id(), e);
            itemResponses.add(BulkItemResponse.failure(slot, indexRequest.opType(), failure));
        }
    }
}
