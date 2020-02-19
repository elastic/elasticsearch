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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportBatchedWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Performs shard-level bulk (index, delete or update) operations
 */
public class TransportBatchedShardBulkAction extends TransportBatchedWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME, BulkShardResponse::new);

    private static final Logger logger = LogManager.getLogger(TransportBatchedShardBulkAction.class);

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final ConcurrentHashMap<ShardId, ShardState> shardQueues = new ConcurrentHashMap<>();

    private final int maxWriteThreads;

    @Inject
    public TransportBatchedShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                           MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper,
                                           ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.WRITE, false);
        this.maxWriteThreads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected TransportRequestOptions transportOptions(Settings settings) {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    private ShardState getOrCreateShardState(ShardId shardId) {
        ShardState queue = shardQueues.get(shardId);
        if (queue == null) {
            ShardState createdQueue = new ShardState(maxWriteThreads);
            ShardState previous = shardQueues.putIfAbsent(shardId, createdQueue);
            queue = Objects.requireNonNullElse(previous, createdQueue);
        }
        return queue;
    }

    private static class ShardState {

        private static final int MAX_QUEUED = 200;

        private final AtomicInteger pendingOps = new AtomicInteger(0);
        private final ConcurrentLinkedQueue<ShardOp> preIndexedQueue = new ConcurrentLinkedQueue<>();
        private final Semaphore writeThreadsSemaphore;

        private ShardState(int maxWriteThreads) {
            writeThreadsSemaphore = new Semaphore(maxWriteThreads);
        }

        private boolean attemptPreIndexedEnqueue(ShardOp shardOp, boolean allowReject) {
            if (allowReject && pendingOps.get() >= MAX_QUEUED) {
                return false;
            } else {
                pendingOps.incrementAndGet();
                preIndexedQueue.add(shardOp);
                return true;
            }
        }

        private ShardOp pollPreIndexed() {
            ShardOp operation = preIndexedQueue.poll();
            if (operation != null) {
                pendingOps.getAndDecrement();
            }
            return operation;
        }

        private boolean shouldScheduleWrites() {
            return writeThreadsSemaphore.tryAcquire();
        }
    }

    public abstract static class ShardOp {

        private final BulkShardRequest request;
        private final IndexShard indexShard;


        public ShardOp(BulkShardRequest request, IndexShard indexShard) {
            this.request = request;
            this.indexShard = indexShard;
        }

        public IndexShard getIndexShard() {
            return indexShard;
        }

        abstract ActionListener<Void> getListener();

        abstract Translog.Location locationToSync();

        abstract void setForcedRefresh(boolean forcedRefresh);

        public BulkShardRequest getRequest() {
            return request;
        }
    }

    public static class PrimaryOp extends ShardOp {

        private final BulkPrimaryExecutionContext context;
        private final PrimaryOpListener listener;

        public PrimaryOp(BulkShardRequest request, IndexShard indexShard,
                         ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener) {
            super(request, indexShard);
            this.context = new BulkPrimaryExecutionContext(request, indexShard);
            this.listener = new PrimaryOpListener(request, context, listener);
        }

        @Override
        public ActionListener<Void> getListener() {
            return listener;
        }

        @Override
        public Translog.Location locationToSync() {
            return context.getLocationToSync();
        }

        @Override
        void setForcedRefresh(boolean forcedRefresh) {
            listener.setForcedRefresh(forcedRefresh);
        }

        private static class PrimaryOpListener implements ActionListener<Void> {

            private final CountDown countDown;
            private final BulkPrimaryExecutionContext context;
            private final ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> delegate;
            private volatile boolean forcedRefresh = false;

            private PrimaryOpListener(BulkShardRequest request, BulkPrimaryExecutionContext context,
                                      ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> delegate) {
                this.context = context;
                this.delegate = delegate;
                if (request.getRefreshPolicy() == WriteRequest.RefreshPolicy.NONE) {
                    countDown = new CountDown(1);
                } else {
                    countDown = new CountDown(2);
                }
            }

            @Override
            public void onResponse(Void v) {
                if (countDown.countDown()) {
                    WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = new WritePrimaryResult<>(context.getBulkShardRequest(),
                        context.buildShardResponse());
                    result.finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
                    delegate.onResponse(result);
                }

            }

            @Override
            public void onFailure(Exception e) {
                boolean finished = countDown.fastForward();
                assert finished;
                delegate.onFailure(e);
            }

            private void setForcedRefresh(boolean forcedRefresh) {
                this.forcedRefresh = forcedRefresh;
            }
        }
    }

    public static class ReplicaOp extends ShardOp {

        private final ReplicaOpListener listener;

        public ReplicaOp(BulkShardRequest request, IndexShard indexShard, ActionListener<ReplicaResult> listener) {
            super(request, indexShard);
            this.listener = new ReplicaOpListener(request, listener);
        }

        @Override
        public ActionListener<Void> getListener() {
            return listener;
        }

        @Override
        Translog.Location locationToSync() {
            return null;
        }

        @Override
        void setForcedRefresh(boolean forcedRefresh) {
            // We do not care on replicas
        }

        private static class ReplicaOpListener implements ActionListener<Void> {

            private final CountDown countDown;
            private final ActionListener<ReplicaResult> delegate;

            private ReplicaOpListener(BulkShardRequest request, ActionListener<ReplicaResult> delegate) {
                this.delegate = delegate;
                if (request.getRefreshPolicy() == WriteRequest.RefreshPolicy.NONE) {
                    countDown = new CountDown(1);
                } else {
                    countDown = new CountDown(2);
                }
            }

            @Override
            public void onResponse(Void v) {
                if (countDown.countDown()) {
                    delegate.onResponse(new ReplicaResult());
                }
            }

            @Override
            public void onFailure(Exception e) {
                boolean finished = countDown.fastForward();
                assert finished;
                delegate.onFailure(e);
            }
        }
    }

    @Override
    protected void shardOperationOnPrimary(BulkShardRequest request, IndexShard primary,
                                           ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener) {
        PrimaryOp shardOp = new PrimaryOp(request, primary, listener);
        enqueueAndSchedule(shardOp, true, true);
    }

    private void enqueueAndSchedule(ShardOp shardOp, boolean isPrimary, boolean allowReject) {
        ShardId shardId = shardOp.getIndexShard().shardId();
        ShardState shardState = getOrCreateShardState(shardId);
        if (shardState.attemptPreIndexedEnqueue(shardOp, allowReject)) {
            maybeSchedule(shardOp.getIndexShard(), shardState, isPrimary);
        } else {
            throw new EsRejectedExecutionException("rejected execution of shard operation", false);
        }
    }

    private void maybeSchedule(IndexShard indexShard, ShardState shardState, boolean isPrimary) {
        if (shardState.shouldScheduleWrites()) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    // TODO: Ensure this cannot happen
                    assert false;
                }

                @Override
                protected void doRun() {
                    performShardOperations(indexShard, isPrimary);
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }

                @Override
                public void onRejection(Exception e) {
                    assert false;
                }

                @Override
                public void onAfter() {
                    shardState.writeThreadsSemaphore.release();
                    if (shardState.preIndexedQueue.isEmpty() == false) {
                        maybeSchedule(indexShard, shardState, isPrimary);
                    }
                }
            });
        }
    }

    // TODO: Consider some type of time slice for indexing
    private static final int MAX_PERFORM_OPS = 10;

    private void performShardOperations(IndexShard indexShard, boolean isPrimary) {
        ShardId shardId = indexShard.shardId();
        ShardState shardState = shardQueues.get(shardId);

        ArrayList<ShardOp> completedOps = new ArrayList<>(MAX_PERFORM_OPS);
        try {
            int opsIndexed = 0;
            ShardOp shardOp;
            while (++opsIndexed <= MAX_PERFORM_OPS && (shardOp = shardState.pollPreIndexed()) != null) {
                boolean opCompleted = true;
                try {
                    if (isPrimary) {
                        if (performPrimaryShardOperation((PrimaryOp) shardOp) == false) {
                            opCompleted = false;
                        }
                    } else {
                        // Do replica
                    }
                } catch (Exception e) {
                    // TODO: Do we want to attempt to complete as many ops as possible so far?
                    opCompleted = false;
                    shardOp.getListener().onFailure(e);
                } finally {
                    if (opCompleted) {
                        completedOps.add(shardOp);
                    }
                }
            }
        } finally {
            finishOperations(indexShard, completedOps);
        }
    }

    private void finishOperations(IndexShard indexShard, ArrayList<ShardOp> completedOps) {
        indexShard.afterWriteOperation();

        ArrayList<ShardOp> completedOpsWaitForRefresh = new ArrayList<>(0);
        ArrayList<ShardOp> completedOpsForceRefresh = new ArrayList<>(0);
        Translog.Location maxLocation = null;

        for (ShardOp indexedOp : completedOps) {
            Translog.Location location = indexedOp.locationToSync();
            if (maxLocation == null) {
                maxLocation = location;
            } else if (location != null && location.compareTo(maxLocation) > 0) {
                maxLocation = location;
            }

            if (indexedOp.getRequest().getRefreshPolicy() == WriteRequest.RefreshPolicy.WAIT_UNTIL) {
                completedOpsWaitForRefresh.add(indexedOp);
            } else if (indexedOp.getRequest().getRefreshPolicy() == WriteRequest.RefreshPolicy.IMMEDIATE) {
                completedOpsForceRefresh.add(indexedOp);
            }
        }

        finishOperations(indexShard, maxLocation, completedOps, completedOpsWaitForRefresh, completedOpsForceRefresh);
    }

    private void finishOperations(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> completedOps,
                                  ArrayList<ShardOp> completedOpsWaitForRefresh, ArrayList<ShardOp> completedOpsForceRefresh) {
        // TODO: Need to improve resiliency around error handling on failed syncs, refreshes, and listener
        //  calls
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST) {
            if (maxLocation != null) {
                try {
                    indexShard.sync(maxLocation, (ex) -> {
                        if (ex == null) {
                            for (ShardOp op : completedOps) {
                                op.getListener().onResponse(null);
                            }
                        } else {
                            for (ShardOp op : completedOps) {
                                // TODO: Check if this is okay
                                op.getListener().onFailure(ex);
                            }
                        }
                    });
                } catch (Exception ex) {
                    for (ShardOp op : completedOps) {
                        // TODO: Check if this is okay
                        op.getListener().onFailure(ex);
                    }
                }
            } else {
                for (ShardOp op : completedOps) {
                    op.getListener().onResponse(null);
                }
            }
        } else {
            for (ShardOp op : completedOps) {
                op.getListener().onResponse(null);
            }
        }

        AtomicBoolean refreshed = new AtomicBoolean(false);
        if (completedOpsWaitForRefresh.isEmpty() == false) {
            if (maxLocation != null) {
                indexShard.addRefreshListener(maxLocation, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn("block until refresh ran out of slots and forced a refresh");
                    }

                    refreshed.set(forcedRefresh);
                    for (ShardOp op : completedOpsWaitForRefresh) {
                        op.setForcedRefresh(forcedRefresh);
                        op.getListener().onResponse(null);
                    }
                });
            } else {
                for (ShardOp op : completedOpsWaitForRefresh) {
                    op.getListener().onResponse(null);
                }
            }
        }

        if (refreshed.get() == false && completedOpsForceRefresh.isEmpty() == false) {
            indexShard.refresh("refresh_flag_index");
            for (ShardOp op : completedOpsForceRefresh) {
                op.setForcedRefresh(true);
                op.getListener().onResponse(null);
            }
        }
    }

    // TODO: Allow rejection after mapping logic
    private static void onShardOperationFailure(PrimaryOp shardOp, Exception e) {
        BulkPrimaryExecutionContext context = shardOp.context;
        while (context.hasMoreOperationsToExecute()) {
            context.setRequestToExecute(context.getCurrent());
            final DocWriteRequest<?> docWriteRequest = context.getRequestToExecute();
            onComplete(
                exceptionToResult(
                    e, shardOp.getIndexShard(), docWriteRequest.opType() == DocWriteRequest.OpType.DELETE, docWriteRequest.version()),
                context, null);
        }
    }

    private boolean performPrimaryShardOperation(PrimaryOp shardOp) throws Exception {
        BulkShardRequest request = shardOp.getRequest();
        Runnable reschedule = () -> enqueueAndSchedule(shardOp, true, false);

        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        return performOnPrimary(shardOp, reschedule, updateHelper, threadPool::absoluteTimeInMillis,
            (update, shardId, mappingListener) -> {
                assert update != null;
                assert shardId != null;
                mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
            },
            mappingUpdateListener -> observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    mappingUpdateListener.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
                }
            }));
    }

    public static boolean performOnPrimary(
        PrimaryOp shardOp,
        Runnable reschedule,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate) throws Exception {

        BulkPrimaryExecutionContext context = shardOp.context;
        while (context.hasMoreOperationsToExecute()) {
            if (executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate,
                reschedule) == false) {
                // We are waiting for a mapping update on another thread, that will invoke this action again once its done
                // so we just break out here.
                return false;
            }
            assert context.isInitial(); // either completed and moved to next or reset
        }
        return true;


    }

    /**
     * Executes bulk item requests and handles request execution exceptions.
     *
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     * a mapping update that will finish and invoke the listener on a different thread
     */
    static boolean executeBulkItemRequest(BulkPrimaryExecutionContext context, UpdateHelper updateHelper,
                                          LongSupplier nowInMillisSupplier, MappingUpdatePerformer mappingUpdater,
                                          Consumer<ActionListener<Void>> waitForMappingUpdate, Runnable rescheduler)
        throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result =
                    new Engine.IndexResult(failure, updateRequest.version());
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            // execute translated update request
            switch (updateResult.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = updateResult.action();
                    IndexMetaData metaData = context.getPrimary().indexSettings().getIndexMetaData();
                    MappingMetaData mappingMd = metaData.mapping();
                    indexRequest.process(metaData.getCreationVersion(), mappingMd, updateRequest.concreteIndex());
                    context.setRequestToExecute(indexRequest);
                    break;
                case DELETED:
                    context.setRequestToExecute(updateResult.action());
                    break;
                case NOOP:
                    context.markOperationAsNoOp(updateResult.action());
                    context.markAsCompleted(context.getExecutionResult());
                    return true;
                default:
                    throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToExecute().version();
        final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
        final Engine.Result result;
        if (isDelete) {
            final DeleteRequest request = context.getRequestToExecute();
            result = primary.applyDeleteOperationOnPrimary(version, request.id(), request.versionType(),
                request.ifSeqNo(), request.ifPrimaryTerm());
        } else {
            final IndexRequest request = context.getRequestToExecute();
            result = primary.applyIndexOperationOnPrimary(version, request.versionType(), new SourceToParse(
                    request.index(), request.id(), request.source(), request.getContentType(), request.routing()),
                request.ifSeqNo(), request.ifPrimaryTerm(), request.getAutoGeneratedTimestamp(), request.isRetry());
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {

            try {
                primary.mapperService().merge(MapperService.SINGLE_MAPPING_NAME,
                    new CompressedXContent(result.getRequiredMappingUpdate(), XContentType.JSON, ToXContent.EMPTY_PARAMS),
                    MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT);
            } catch (Exception e) {
                logger.info(() -> new ParameterizedMessage("{} mapping update rejected by primary", primary.shardId()), e);
                onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                return true;
            }

            mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void v) {
                        context.markAsRequiringMappingUpdate();
                        waitForMappingUpdate.accept(
                            ActionListener.runAfter(new ActionListener<>() {
                                @Override
                                public void onResponse(Void v) {
                                    assert context.requiresWaitingForMappingUpdate();
                                    context.resetForExecutionForRetry();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    context.failOnMappingUpdate(e);
                                }
                            }, rescheduler)
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                        // Requesting mapping update failed, so we don't have to wait for a cluster state update
                        assert context.isInitial();
                        rescheduler.run();
                    }
                });
            return false;
        } else {
            onComplete(result, context, updateResult);
        }
        return true;
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version) {
        return isDelete ? primary.getFailedDeleteResult(e, version) : primary.getFailedIndexResult(e, version);
    }

    private static void onComplete(Engine.Result r, BulkPrimaryExecutionContext context, UpdateHelper.Result updateResult) {
        context.markOperationAsExecuted(r);
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        if (isUpdate && isFailed && isConflictException(executionResult.getFailure().getCause())
            && context.getRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForExecutionForRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            response = processUpdateResponse((UpdateRequest) docWriteRequest, context.getConcreteIndex(), executionResult, updateResult);
        } else {
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                final MessageSupplier messageSupplier = () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(), opType.getLowercase(), docWriteRequest);
                if (TransportBatchedShardBulkAction.isConflictException(failure)) {
                    logger.trace(messageSupplier, failure);
                } else {
                    logger.debug(messageSupplier, failure);
                }
            }
            response = executionResult;
        }
        context.markAsCompleted(response);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    private static BulkItemResponse processUpdateResponse(final UpdateRequest updateRequest, final String concreteIndex,
                                                          BulkItemResponse operationResponse, final UpdateHelper.Result translate) {
        final BulkItemResponse response;
        if (operationResponse.isFailed()) {
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, operationResponse.getFailure());
        } else {
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(),
                    indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(), indexResponse.getResult());

                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<XContentType, Map<String, Object>> sourceAndContent =
                        XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());
                    updateResponse.setGetResult(UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                        indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                        indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                }
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(),
                    deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(), deleteResponse.getResult());

                final GetResult getResult = UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                    deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(), deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(), translate.updateSourceContentType(), null);

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }

    @Override
    public WriteReplicaResult shardOperationOnReplica(BulkShardRequest request, IndexShard replica) {
        throw new AssertionError("Override the async method, so the synchronous method should not be called");
    }

    @Override
    protected void shardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        try {
            ReplicaOp shardOp = new ReplicaOp(request, replica, listener);
            enqueueAndSchedule(shardOp, false, true);

//            final Translog.Location location = performOnReplica(request, replica);
//            listener.onResponse(new WriteReplicaResult(null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            final BulkItemRequest item = request.items()[i];
            final BulkItemResponse response = item.getPrimaryResponse();
            final Engine.Result operationResult;
            if (item.getPrimaryResponse().isFailed()) {
                if (response.getFailure().getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    continue; // ignore replication as we didn't generate a sequence number for this request.
                }

                final long primaryTerm;
                if (response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                    // primary is on older version, just take the current primary term
                    primaryTerm = replica.getOperationPrimaryTerm();
                } else {
                    primaryTerm = response.getFailure().getTerm();
                }
                operationResult = replica.markSeqNoAsNoop(response.getFailure().getSeqNo(), primaryTerm,
                    response.getFailure().getMessage());
            } else {
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    continue; // ignore replication as it's a noop
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                operationResult = performOpOnReplica(response.getResponse(), item.request(), replica);
            }
            assert operationResult != null : "operation result must never be null when primary response has no failure";
            location = syncOperationResultOrThrow(operationResult, location);
        }
        return location;
    }

    private static Engine.Result performOpOnReplica(DocWriteResponse primaryResponse, DocWriteRequest<?> docWriteRequest,
                                                    IndexShard replica) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE:
            case INDEX:
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final ShardId shardId = replica.shardId();
                final SourceToParse sourceToParse = new SourceToParse(shardId.getIndexName(), indexRequest.id(),
                    indexRequest.source(), indexRequest.getContentType(), indexRequest.routing());
                result = replica.applyIndexOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(), indexRequest.getAutoGeneratedTimestamp(), indexRequest.isRetry(), sourceToParse);
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(), deleteRequest.id());
                break;
            default:
                assert false : "Unexpected request operation type on replica: " + docWriteRequest + ";primary result: " + primaryResponse;
                throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // Even though the primary waits on all nodes to ack the mapping changes to the master
            // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
            // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
            // mapping update. Assume that that update is first applied on the primary, and only later on the replica
            // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
            // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
            // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
            // applied the new mapping, so there is no other option than to wait.
            throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
        }
        return result;
    }
}
