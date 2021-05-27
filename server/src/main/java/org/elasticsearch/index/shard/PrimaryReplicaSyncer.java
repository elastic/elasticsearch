/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class PrimaryReplicaSyncer {

    private static final Logger logger = LogManager.getLogger(PrimaryReplicaSyncer.class);

    private final TransportService transportService;
    private final SyncAction syncAction;

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    @Inject
    public PrimaryReplicaSyncer(TransportService transportService, TransportResyncReplicationAction syncAction) {
        this(transportService, (SyncAction) syncAction);
    }

    // for tests
    public PrimaryReplicaSyncer(TransportService transportService, SyncAction syncAction) {
        this.transportService = transportService;
        this.syncAction = syncAction;
    }

    void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void resync(final IndexShard indexShard, final ActionListener<ResyncTask> listener) {
        Translog.Snapshot snapshot = null;
        try {
            final long startingSeqNo = indexShard.getLastKnownGlobalCheckpoint() + 1;
            assert startingSeqNo >= 0 : "startingSeqNo must be non-negative; got [" + startingSeqNo + "]";
            final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
            final ShardId shardId = indexShard.shardId();
            // Wrap translog snapshot to make it synchronized as it is accessed by different threads through SnapshotSender.
            // Even though those calls are not concurrent, snapshot.next() uses non-synchronized state and is not multi-thread-compatible
            // Also fail the resync early if the shard is shutting down
            snapshot = indexShard.newChangesSnapshot("resync", startingSeqNo, Long.MAX_VALUE, false, false);
            final Translog.Snapshot originalSnapshot = snapshot;
            final Translog.Snapshot wrappedSnapshot = new Translog.Snapshot() {
                @Override
                public synchronized void close() throws IOException {
                    originalSnapshot.close();
                }

                @Override
                public synchronized int totalOperations() {
                    return originalSnapshot.totalOperations();
                }

                @Override
                public synchronized Translog.Operation next() throws IOException {
                    IndexShardState state = indexShard.state();
                    if (state == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(shardId);
                    } else {
                        assert state == IndexShardState.STARTED : "resync should only happen on a started shard, but state was: " + state;
                    }
                    return originalSnapshot.next();
                }
            };
            final ActionListener<ResyncTask> resyncListener = new ActionListener<ResyncTask>() {
                @Override
                public void onResponse(final ResyncTask resyncTask) {
                    try {
                        wrappedSnapshot.close();
                        listener.onResponse(resyncTask);
                    } catch (final Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    try {
                        wrappedSnapshot.close();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    } finally {
                        listener.onFailure(e);
                    }
                }
            };
            // We must capture the timestamp after snapshotting a snapshot of operations to make sure
            // that the auto_id_timestamp of every operation in the snapshot is at most this value.
            final long maxSeenAutoIdTimestamp = indexShard.getMaxSeenAutoIdTimestamp();
            resync(shardId, indexShard.routingEntry().allocationId().getId(), indexShard.getPendingPrimaryTerm(), wrappedSnapshot,
                startingSeqNo, maxSeqNo, maxSeenAutoIdTimestamp, resyncListener);
        } catch (Exception e) {
            try {
                IOUtils.close(snapshot);
            } catch (IOException inner) {
                e.addSuppressed(inner);
            } finally {
                listener.onFailure(e);
            }
        }
    }

    private void resync(final ShardId shardId, final String primaryAllocationId, final long primaryTerm, final Translog.Snapshot snapshot,
                        long startingSeqNo, long maxSeqNo, long maxSeenAutoIdTimestamp, ActionListener<ResyncTask> listener) {
        ResyncRequest request = new ResyncRequest(shardId, primaryAllocationId);
        final TaskManager taskManager = transportService.getTaskManager();
        ResyncTask resyncTask = (ResyncTask) taskManager.register("transport", "resync", request); // it's not transport :-)
        ActionListener<Void> wrappedListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void ignore) {
                resyncTask.setPhase("finished");
                taskManager.unregister(resyncTask);
                listener.onResponse(resyncTask);
            }

            @Override
            public void onFailure(Exception e) {
                resyncTask.setPhase("finished");
                taskManager.unregister(resyncTask);
                listener.onFailure(e);
            }
        };
        try {
            new SnapshotSender(syncAction, resyncTask, shardId, primaryAllocationId, primaryTerm, snapshot, chunkSize.bytesAsInt(),
                startingSeqNo, maxSeqNo, maxSeenAutoIdTimestamp, transportService.getThreadPool().generic(), wrappedListener).run();
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }

    public interface SyncAction {
        void sync(ResyncReplicationRequest request, Task parentTask, String primaryAllocationId, long primaryTerm,
                  ActionListener<ResyncReplicationResponse> listener);
    }

    static class SnapshotSender extends AbstractRunnable implements ActionListener<ResyncReplicationResponse> {
        private final Logger logger;
        private final SyncAction syncAction;
        private final ResyncTask task; // to track progress
        private final String primaryAllocationId;
        private final long primaryTerm;
        private final ShardId shardId;
        private final Translog.Snapshot snapshot;
        private final Executor executor; // executor to fork to for reading and then sending ops from the snapshot
        private final long startingSeqNo;
        private final long maxSeqNo;
        private final long maxSeenAutoIdTimestamp;
        private final int chunkSizeInBytes;
        private final ActionListener<Void> listener;
        private final AtomicBoolean firstMessage = new AtomicBoolean(true);
        private final AtomicInteger totalSentOps = new AtomicInteger();
        private final AtomicInteger totalSkippedOps = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        SnapshotSender(SyncAction syncAction, ResyncTask task, ShardId shardId, String primaryAllocationId, long primaryTerm,
                       Translog.Snapshot snapshot, int chunkSizeInBytes, long startingSeqNo, long maxSeqNo,
                       long maxSeenAutoIdTimestamp, Executor executor, ActionListener<Void> listener) {
            this.logger = PrimaryReplicaSyncer.logger;
            this.syncAction = syncAction;
            this.task = task;
            this.shardId = shardId;
            this.primaryAllocationId = primaryAllocationId;
            this.primaryTerm = primaryTerm;
            this.snapshot = snapshot;
            this.chunkSizeInBytes = chunkSizeInBytes;
            this.startingSeqNo = startingSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.executor = executor;
            this.listener = listener;
            task.setTotalOperations(snapshot.totalOperations());
        }

        @Override
        public void onResponse(ResyncReplicationResponse response) {
            executor.execute(this);
        }

        @Override
        public void onFailure(Exception e) {
            if (closed.compareAndSet(false, true)) {
                executor.execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception ex) {
                        e.addSuppressed(ex);

                        // We are on the generic threadpool so shouldn't be rejected, and listener#onFailure shouldn't throw anything,
                        // so getting here should be impossible.
                        assert false : e;

                        // Notify the listener on the current thread anyway, just in case.
                        listener.onFailure(e);
                    }

                    @Override
                    protected void doRun() {
                        listener.onFailure(e);
                    }
                });
            }
        }

        private static final Translog.Operation[] EMPTY_ARRAY = new Translog.Operation[0];

        @Override
        protected void doRun() throws Exception {
            long size = 0;
            final List<Translog.Operation> operations = new ArrayList<>();

            task.setPhase("collecting_ops");
            task.setResyncedOperations(totalSentOps.get());
            task.setSkippedOperations(totalSkippedOps.get());

            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                final long seqNo = operation.seqNo();
                if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo) {
                    totalSkippedOps.incrementAndGet();
                    continue;
                }
                assert operation.seqNo() >= 0 : "sending operation with unassigned sequence number [" + operation + "]";
                operations.add(operation);
                size += operation.estimateSize();
                totalSentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (size >= chunkSizeInBytes) {
                    break;
                }
            }
            final long trimmedAboveSeqNo = firstMessage.get() ? maxSeqNo : SequenceNumbers.UNASSIGNED_SEQ_NO;
            // have to send sync request even in case of there are no operations to sync - have to sync trimmedAboveSeqNo at least
            if (operations.isEmpty() == false || trimmedAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                task.setPhase("sending_ops");
                ResyncReplicationRequest request =
                    new ResyncReplicationRequest(shardId, trimmedAboveSeqNo, maxSeenAutoIdTimestamp, operations.toArray(EMPTY_ARRAY));
                logger.trace("{} sending batch of [{}][{}] (total sent: [{}], skipped: [{}])", shardId, operations.size(),
                    new ByteSizeValue(size), totalSentOps.get(), totalSkippedOps.get());
                firstMessage.set(false);
                syncAction.sync(request, task, primaryAllocationId, primaryTerm, this);
            } else if (closed.compareAndSet(false, true)) {
                logger.trace("{} resync completed (total sent: [{}], skipped: [{}])", shardId, totalSentOps.get(), totalSkippedOps.get());
                listener.onResponse(null);
            }
        }
    }

    public static class ResyncRequest extends ActionRequest {

        private final ShardId shardId;
        private final String allocationId;

        public ResyncRequest(ShardId shardId, String allocationId) {
            this.shardId = shardId;
            this.allocationId = allocationId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ResyncTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return toString();
        }

        @Override
        public String toString() {
            return "ResyncRequest{ " + shardId + ", " + allocationId + " }";
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class ResyncTask extends Task {
        private volatile String phase = "starting";
        private volatile int totalOperations;
        private volatile int resyncedOperations;
        private volatile int skippedOperations;

        public ResyncTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
        }

        /**
         * Set the current phase of the task.
         */
        public void setPhase(String phase) {
            this.phase = phase;
        }

        /**
         * Get the current phase of the task.
         */
        public String getPhase() {
            return phase;
        }

        /**
         * total number of translog operations that were captured by translog snapshot
         */
        public int getTotalOperations() {
            return totalOperations;
        }

        public void setTotalOperations(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        /**
         * number of operations that have been successfully replicated
         */
        public int getResyncedOperations() {
            return resyncedOperations;
        }

        public void setResyncedOperations(int resyncedOperations) {
            this.resyncedOperations = resyncedOperations;
        }

        /**
         * number of translog operations that have been skipped
         */
        public int getSkippedOperations() {
            return skippedOperations;
        }

        public void setSkippedOperations(int skippedOperations) {
            this.skippedOperations = skippedOperations;
        }

        @Override
        public ResyncTask.Status getStatus() {
            return new ResyncTask.Status(phase, totalOperations, resyncedOperations, skippedOperations);
        }

        public static class Status implements Task.Status {
            public static final String NAME = "resync";

            private final String phase;
            private final int totalOperations;
            private final int resyncedOperations;
            private final int skippedOperations;

            public Status(StreamInput in) throws IOException {
                phase = in.readString();
                totalOperations = in.readVInt();
                resyncedOperations = in.readVInt();
                skippedOperations = in.readVInt();
            }

            public Status(String phase, int totalOperations, int resyncedOperations, int skippedOperations) {
                this.phase = requireNonNull(phase, "Phase cannot be null");
                this.totalOperations = totalOperations;
                this.resyncedOperations = resyncedOperations;
                this.skippedOperations = skippedOperations;
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("phase", phase);
                builder.field("totalOperations", totalOperations);
                builder.field("resyncedOperations", resyncedOperations);
                builder.field("skippedOperations", skippedOperations);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(phase);
                out.writeVLong(totalOperations);
                out.writeVLong(resyncedOperations);
                out.writeVLong(skippedOperations);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }


            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Status status = (Status) o;

                if (totalOperations != status.totalOperations) return false;
                if (resyncedOperations != status.resyncedOperations) return false;
                if (skippedOperations != status.skippedOperations) return false;
                return phase.equals(status.phase);
            }

            @Override
            public int hashCode() {
                int result = phase.hashCode();
                result = 31 * result + totalOperations;
                result = 31 * result + resyncedOperations;
                result = 31 * result + skippedOperations;
                return result;
            }
        }
    }
}
