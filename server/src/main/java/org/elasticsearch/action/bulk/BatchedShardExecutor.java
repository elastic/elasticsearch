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
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class BatchedShardExecutor {

    private static final long MAX_EXECUTE_NANOS = TimeUnit.MILLISECONDS.toNanos(15);
    private static final String CLOSED_SHARD_MESSAGE = "Cannot perform operation, IndexShard is closed.";

    private static final Logger logger = LogManager.getLogger(BatchedShardExecutor.class);

    private final CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler;
    private final CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler;
    private final ThreadPool threadPool;
    private final int numberOfWriteThreads;

    private final ConcurrentHashMap<ShardId, ShardState> shardState = new ConcurrentHashMap<>();


    public BatchedShardExecutor(ClusterService clusterService, ThreadPool threadPool, UpdateHelper updateHelper,
                                MappingUpdatedAction mappingUpdatedAction) {
        this(primaryOpHandler(clusterService, threadPool, updateHelper, mappingUpdatedAction), replicaOpHandler(), threadPool);
    }

    public BatchedShardExecutor(CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler,
                                CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler, ThreadPool threadPool) {
        this.primaryOpHandler = primaryOpHandler;
        this.replicaOpHandler = replicaOpHandler;
        this.threadPool = threadPool;
        this.numberOfWriteThreads = threadPool.info(ThreadPool.Names.WRITE).getMax();
    }

    public void primary(BulkShardRequest request, IndexShard primary, ActionListener<WriteResult> writeListener,
                        ActionListener<FlushResult> flushListener) {
        PrimaryOp shardOp = new PrimaryOp(request, primary, writeListener, flushListener);
        enqueueAndSchedule(shardOp, true, true);
    }

    public void replica(BulkShardRequest request, IndexShard primary, ActionListener<Void> writeListener,
                        ActionListener<FlushResult> flushListener) {
        ReplicaOp shardOp = new ReplicaOp(request, primary, writeListener, flushListener);
        enqueueAndSchedule(shardOp, false, true);
    }

    ShardState getShardState(ShardId shardId) {
        return shardState.get(shardId);
    }

    private void enqueueAndSchedule(ShardOp shardOp, boolean isPrimary, boolean allowReject) {
        ShardId shardId = shardOp.getIndexShard().shardId();
        ShardState shardState = getOrCreateShardState(shardId);
        if (shardState.attemptPreIndexedEnqueue(shardOp, allowReject)) {
            // If the ShardState was closed after we enqueued, attempt to remove our operation and finish it
            // to ensure that it does not get lost.
            if (shardState.isClosed() && shardState.remove(shardOp)) {
                onFailure(Stream.of(shardOp.getWriteListener()), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
            } else {
                maybeSchedule(shardOp.getIndexShard(), shardState, isPrimary);
            }
        } else {
            throw new EsRejectedExecutionException("rejected execution of shard operation", false);
        }
    }

    private ShardState getOrCreateShardState(ShardId shardId) {
        ShardState queue = shardState.get(shardId);
        if (queue == null) {
            ShardState createdQueue = new ShardState(numberOfWriteThreads);
            ShardState previous = shardState.putIfAbsent(shardId, createdQueue);
            queue = Objects.requireNonNullElse(previous, createdQueue);
        }
        return queue;
    }

    private void maybeSchedule(IndexShard indexShard, ShardState shardState, boolean isPrimary) {
        if (shardState.shouldScheduleWriteTask()) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    logger.error("Uncaught exception when handling shard operations", e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    shardState.markTaskStarted();
                    performShardOperations(indexShard, isPrimary);
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }

                @Override
                public void onRejection(Exception e) {
                    assert false : e;
                }

                @Override
                public void onAfter() {
                    if (shardState.preIndexedQueue.isEmpty() == false) {
                        maybeSchedule(indexShard, shardState, isPrimary);
                    }
                }
            });
        }
    }

    private void performShardOperations(IndexShard indexShard, boolean isPrimary) {
        ShardId shardId = indexShard.shardId();
        ShardState shardState = this.shardState.get(shardId);
        if (shardState == null) {
            // The IndexShard has closed and the resources have been cleaned
            return;
        }

        ArrayList<ShardOp> completedOps = new ArrayList<>();
        try {
            int opsIndexed = 0;
            ShardOp shardOp;
            long startNanos = System.nanoTime();
            long nanosSpentExecuting = 0;
            while ((nanosSpentExecuting < MAX_EXECUTE_NANOS) && (shardOp = shardState.pollPreIndexed()) != null) {
                boolean opCompleted = false;
                try {
                    if (isPrimary) {
                        PrimaryOp primaryOp = (PrimaryOp) shardOp;
                        Runnable rescheduler = () -> enqueueAndSchedule(primaryOp, true, false);
                        opCompleted = primaryOpHandler.apply(primaryOp, rescheduler);
                    } else {
                        opCompleted = replicaOpHandler.apply((ReplicaOp) shardOp);
                    }
                } catch (Exception e) {
                    onFailure(Stream.of(shardOp.getWriteListener()), e);
                } finally {
                    ++opsIndexed;
                    // Update nanosSpentExecuting every 8 operations
                    if ((opsIndexed & (8 - 1)) == 0) {
                        nanosSpentExecuting = System.nanoTime() - startNanos;
                    }
                    if (opCompleted) {
                        completedOps.add(shardOp);
                    }
                }
            }
        } finally {
            if (completedOps.isEmpty() == false) {
                finishOperations(indexShard, completedOps);
            }
            cleanupIfShardClosed(indexShard);
        }
    }

    private void finishOperations(IndexShard indexShard, ArrayList<ShardOp> completedOps) {
        // Notify the write listeners
        onResponse(completedOps.stream().map(ShardOp::getWriteListener), null);

        indexShard.afterWriteOperation();

        ArrayList<ShardOp> completedOpsAlreadySynced = new ArrayList<>();
        ArrayList<ShardOp> completedOpsNeedSync = new ArrayList<>();
        ArrayList<ShardOp> completedOpsWaitForRefresh = new ArrayList<>(0);
        ArrayList<ShardOp> completedOpsForceRefresh = new ArrayList<>(0);

        Translog.Location maxLocation = null;
        Translog.Location syncedLocation = null;
        try {
            syncedLocation = indexShard.getTranslogLastSyncedLocation();
        } catch (Exception e) {
            // The Translog might have closed. Ignore.
        }

        for (ShardOp indexedOp : completedOps) {
            Translog.Location location = indexedOp.locationToSync();
            if (location != null) {
                if (syncedLocation == null || location.compareTo(syncedLocation) > 0) {
                    completedOpsNeedSync.add(indexedOp);
                } else {
                    completedOpsAlreadySynced.add(indexedOp);
                }
                if (maxLocation == null) {
                    maxLocation = location;
                } else if (location.compareTo(maxLocation) > 0) {
                    maxLocation = location;
                }
            } else {
                completedOpsAlreadySynced.add(indexedOp);
            }

            if (indexedOp.getRequest().getRefreshPolicy() == WriteRequest.RefreshPolicy.WAIT_UNTIL) {
                completedOpsWaitForRefresh.add(indexedOp);
            } else if (indexedOp.getRequest().getRefreshPolicy() == WriteRequest.RefreshPolicy.IMMEDIATE) {
                completedOpsForceRefresh.add(indexedOp);
                indexedOp.getFlushListener().setForcedRefresh(true);
            }
        }

        onResponse(completedOpsAlreadySynced.stream().map(ShardOp::getFlushListener), null);

        finishOperations(indexShard, maxLocation, completedOpsNeedSync, completedOpsWaitForRefresh, completedOpsForceRefresh);
    }

    private void finishOperations(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> completedOpsNeedSync,
                                  ArrayList<ShardOp> completedOpsWaitForRefresh, ArrayList<ShardOp> completedOpsForceRefresh) {
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST && maxLocation != null) {
            syncOperations(indexShard, maxLocation, completedOpsNeedSync);
        } else {
            onResponse(completedOpsNeedSync.stream().map(ShardOp::getFlushListener), null);
        }

        AtomicBoolean refreshed = new AtomicBoolean(false);
        if (completedOpsWaitForRefresh.isEmpty() == false) {
            if (maxLocation != null) {
                // TODO: Do we want to add each listener individually?
                addRefreshListeners(indexShard, maxLocation, completedOpsWaitForRefresh);
            } else {
                onResponse(completedOpsWaitForRefresh.stream().map(ShardOp::getFlushListener), null);
            }
        }

        if (completedOpsForceRefresh.isEmpty() == false) {
            if (refreshed.get() == false) {
                forceRefresh(indexShard);
            }
            onResponse(completedOpsForceRefresh.stream().map(ShardOp::getFlushListener), null);
        }
    }

    // TODO: Confirm if we want WARN log level. A lot of these will be thrown when the shard is closed/closing

    private void syncOperations(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> operations) {
        try {
            indexShard.sync(maxLocation, (ex) -> {
                if (ex == null) {
                    onResponse(operations.stream().map(ShardOp::getFlushListener), null);
                } else {
                    onFailure(operations.stream().map(ShardOp::getFlushListener), ex);
                }
            });
        } catch (Exception ex) {
            logger.warn("exception while syncing shard operations", ex);
            onFailure(operations.stream().map(ShardOp::getFlushListener), ex);
        }
    }

    private void addRefreshListeners(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> operations) {
        try {
            indexShard.addRefreshListener(maxLocation, forcedRefresh -> {
                if (forcedRefresh) {
                    logger.warn("block until refresh ran out of slots and forced a refresh");
                }

                operations.forEach(op -> op.flushListener.setForcedRefresh(forcedRefresh));
                onResponse(operations.stream().map(ShardOp::getFlushListener), null);
            });
        } catch (Exception ex) {
            logger.warn("exception while adding refresh listener for shard operations", ex);
        }
    }

    private void forceRefresh(IndexShard indexShard) {
        try {
            indexShard.refresh("refresh_flag_index");
        } catch (Exception ex) {
            logger.warn("exception while forcing immediate refresh for shard operation", ex);
        }
    }

    private void cleanupIfShardClosed(IndexShard indexShard) {
        if (indexShard.state() == IndexShardState.CLOSED) {
            ShardState removed = shardState.remove(indexShard.shardId());
            // If we did not successfully remove the ShardState, another thread did and will handling the
            // closing.
            if (removed != null) {
                removed.close();
                ShardOp shardOp;
                ArrayList<ActionListener<Void>> writeListeners = new ArrayList<>();
                while ((shardOp = removed.pollPreIndexed()) != null) {
                    writeListeners.add(shardOp.getWriteListener());
                }
                onFailure(writeListeners.stream(), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
            }
        }
    }

    private <T> void onResponse(Stream<ActionListener<T>> listenerStream, T value) {
        try {
            ActionListener.onResponse(listenerStream, value);
        } catch (Exception e) {
            logger.error("uncaught exception when notifying shard operation listeners", e);
        }
    }

    private <T> void onFailure(Stream<ActionListener<T>> listenerStream, Exception ex) {
        try {
            ActionListener.onFailure(listenerStream, ex);
        } catch (Exception e) {
            logger.error("uncaught exception when notifying shard operation listeners", e);
        }
    }

    private static CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler(
        ClusterService clusterService,
        ThreadPool threadPool,
        UpdateHelper updateHelper,
        MappingUpdatedAction mappingUpdatedAction) {
        return (primaryOp, rescheduler) -> {
            TimeValue timeout = primaryOp.getRequest().timeout();
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
            return TransportShardBulkAction.executeBulkItemRequests(primaryOp.context, updateHelper, threadPool::absoluteTimeInMillis,
                (update, shardId, mappingListener) -> {
                    assert update != null;
                    assert shardId != null;
                    mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
                }, TransportShardBulkAction.waitForMappingUpdate(observer, clusterService), rescheduler);
        };
    }

    private static CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler() {
        return (replicaOp) -> {
            Translog.Location location = TransportShardBulkAction.performOnReplica(replicaOp.getRequest(), replicaOp.getIndexShard());
            replicaOp.setLocation(location);
            return true;
        };
    }

    static class ShardState {

        private static final int MAX_QUEUED = 400;

        private final int maxScheduledTasks;
        private final AtomicInteger pendingOps = new AtomicInteger(0);
        private final ConcurrentLinkedQueue<ShardOp> preIndexedQueue = new ConcurrentLinkedQueue<>();
        private final Semaphore scheduleTaskSemaphore;
        private volatile boolean isClosed = false;

        private ShardState(int maxScheduledTasks) {
            scheduleTaskSemaphore = new Semaphore(maxScheduledTasks);
            this.maxScheduledTasks = maxScheduledTasks;
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

        private boolean remove(ShardOp shardOp) {
            if (preIndexedQueue.remove(shardOp)) {
                pendingOps.getAndDecrement();
                return true;
            }
            return false;
        }

        private ShardOp pollPreIndexed() {
            ShardOp operation = preIndexedQueue.poll();
            if (operation != null) {
                pendingOps.getAndDecrement();
            }
            return operation;
        }

        private boolean shouldScheduleWriteTask() {
            return scheduleTaskSemaphore.tryAcquire();
        }

        private void markTaskStarted() {
            scheduleTaskSemaphore.release();
        }

        int pendingOperations() {
            return pendingOps.get();
        }

        int scheduledTasks() {
            return maxScheduledTasks - scheduleTaskSemaphore.availablePermits();
        }

        boolean isClosed() {
            return isClosed;
        }

        void close() {
            isClosed = true;
        }
    }

    public static class WriteResult {

        @Nullable
        private final BulkShardRequest replicaRequest;
        private final BulkShardResponse response;


        public WriteResult(BulkShardRequest replicaRequest, BulkShardResponse response) {
            this.replicaRequest = replicaRequest;
            this.response = response;
        }

        public BulkShardRequest getReplicaRequest() {
            return replicaRequest;
        }

        public BulkShardResponse getResponse() {
            return response;
        }
    }

    public static class FlushResult {

        private final boolean forcedRefresh;

        public FlushResult(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }

        public boolean forcedRefresh() {
            return forcedRefresh;
        }
    }

    public abstract static class ShardOp {

        private final BulkShardRequest request;
        private final IndexShard indexShard;
        private final FlushListener flushListener;

        public ShardOp(BulkShardRequest request, IndexShard indexShard, ActionListener<FlushResult> flushListener) {
            this.request = request;
            this.indexShard = indexShard;
            this.flushListener = new FlushListener(request.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE, flushListener);
        }

        IndexShard getIndexShard() {
            return indexShard;
        }

        FlushListener getFlushListener() {
            return flushListener;
        }

        abstract ActionListener<Void> getWriteListener();

        abstract Translog.Location locationToSync();

        BulkShardRequest getRequest() {
            return request;
        }

        private static class FlushListener implements ActionListener<Void> {

            private final CountDown countDown;
            private final ActionListener<FlushResult> delegate;
            private volatile boolean forcedRefresh;

            private FlushListener(boolean waitOnRefresh, ActionListener<FlushResult> delegate) {
                this.delegate = delegate;
                if (waitOnRefresh) {
                    countDown = new CountDown(2);
                } else {
                    countDown = new CountDown(1);
                }
            }

            @Override
            public void onResponse(Void v) {
                if (countDown.countDown()) {
                    delegate.onResponse(new FlushResult(forcedRefresh));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (countDown.fastForward()) {
                    delegate.onFailure(e);
                }
            }

            void setForcedRefresh(boolean forcedRefresh) {
                this.forcedRefresh = forcedRefresh;
            }
        }
    }

    public static class PrimaryOp extends ShardOp {

        private final BulkPrimaryExecutionContext context;
        private final ActionListener<Void> writeListener;

        public PrimaryOp(BulkShardRequest request, IndexShard indexShard, ActionListener<WriteResult> writeListener,
                         ActionListener<FlushResult> flushListener) {
            super(request, indexShard, flushListener);
            this.context = new BulkPrimaryExecutionContext(request, indexShard);
            this.writeListener = new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    writeListener.onResponse(new WriteResult(context.getBulkShardRequest(), context.buildShardResponse()));
                }

                @Override
                public void onFailure(Exception e) {
                    writeListener.onFailure(e);
                }
            };
        }

        @Override
        ActionListener<Void> getWriteListener() {
            return writeListener;
        }

        @Override
        Translog.Location locationToSync() {
            return context.getLocationToSync();
        }

        BulkPrimaryExecutionContext getContext() {
            return context;
        }
    }

    public static class ReplicaOp extends ShardOp {

        private final ActionListener<Void> listener;
        private Translog.Location location;

        public ReplicaOp(BulkShardRequest request, IndexShard indexShard, ActionListener<Void> listener,
                         ActionListener<FlushResult> flushListener) {
            super(request, indexShard, flushListener);
            this.listener = listener;
        }

        @Override
        ActionListener<Void> getWriteListener() {
            return listener;
        }

        @Override
        Translog.Location locationToSync() {
            return this.location;
        }

        void setLocation(Translog.Location location) {
            this.location = location;
        }
    }
}
