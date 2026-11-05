/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardProgress;
import org.elasticsearch.xpack.downsample.DownsampleShardIndexerException;
import org.elasticsearch.xpack.downsample.DownsampleShardTaskParams;
import org.elasticsearch.xpack.downsample.ShardTaskReporter;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * One local run of a shard task on the node that owns it. It is the {@link ShardTaskReporter} of the indexer, and it is the
 * only writer of the shard task document on this node while it runs:
 * <ol>
 *     <li>it claims the unassigned task, or resumes a task this node already owns (e.g. after a restart), by marking it
 *     {@code RUNNING} with a new run id, then starts the indexer. A claim that loses the race to another node ends the run;</li>
 *     <li>after every bulk of the indexer it writes the progress, coalesced: at most one write in flight, the newest counters
 *     are written next;</li>
 *     <li>when the indexer is done it writes {@code COMPLETED} or {@code FAILED} with the final counters.</li>
 * </ol>
 * The writes are serialized, each one a compare-and-set on the version of the previous one. On a version conflict the run
 * re-reads the document: if the task is still owned by this node at the same epoch (a benign concurrent write) it retries,
 * otherwise the task was taken away (fenced) and the run cancels itself without writing. A run that cannot write its final
 * state is simply dropped: the task stays {@code RUNNING} on this node, so the poller starts it again.
 */
final class ShardTaskRun implements ShardTaskReporter {

    private static final Logger logger = LogManager.getLogger(ShardTaskRun.class);
    private static final int MAX_CONFLICT_RETRIES = 3;

    private final DownsampleStateStore.NodeView store;
    private final ProjectId projectId;
    private final String docId;
    private final long epoch;
    private final boolean claim;
    private final String runId = UUIDs.randomBase64UUID();
    private final DownsampleShardTaskParams params;
    private final DownsampleShardProgress progress;
    private final Consumer<ShardTaskRun> onDone;
    private final AtomicBoolean done = new AtomicBoolean();
    private volatile boolean cancelled;
    private volatile boolean started;

    // the write queue, guarded by this
    private Versioned<ShardTaskDoc> current;
    private boolean writing;
    private boolean progressPending;
    private boolean finishing;
    private PendingWrite finalWrite;

    private record PendingWrite(UnaryOperator<ShardTaskDoc> change, ActionListener<Void> listener) {}

    /**
     * @param seen   the shard task as found by the poller: unassigned, to claim, or running on this node, to resume
     * @param onDone called once when the run is over, whatever the outcome
     */
    ShardTaskRun(DownsampleStateStore.NodeView store, Versioned<ShardTaskDoc> seen, Consumer<ShardTaskRun> onDone) {
        this.store = store;
        this.projectId = seen.doc().projectId();
        this.docId = seen.doc().id();
        this.claim = switch (seen.doc().status()) {
            case UNASSIGNED -> true;
            case RUNNING -> false;
            case COMPLETED, FAILED -> throw new IllegalArgumentException(
                "shard task [" + docId + "] is [" + seen.doc().status() + "], there is nothing to run"
            );
        };
        // a claim moves the task to the next epoch
        this.epoch = claim ? seen.doc().epoch() + 1 : seen.doc().epoch();
        this.params = seen.doc().params();
        this.progress = new DownsampleShardProgress(params.shardId(), params.indexStartTimeMillis(), params.indexEndTimeMillis());
        this.onDone = onDone;
        this.current = seen;
    }

    ProjectId projectId() {
        return projectId;
    }

    String docId() {
        return docId;
    }

    long epoch() {
        return epoch;
    }

    String runId() {
        return runId;
    }

    DownsampleShardTaskParams params() {
        return params;
    }

    /**
     * Whether this run claims the task, as opposed to resuming a task this node already owns.
     */
    boolean isClaim() {
        return claim;
    }

    /**
     * Whether the run owns the task in the state index: its claim or resume write succeeded. Until then the document does not show
     * this run, so it cannot be judged by it.
     */
    boolean hasStarted() {
        return started;
    }

    /**
     * Claims or resumes the task, then runs the indexer with {@code indexer}, which must notify its listener when the indexer is
     * done.
     */
    void start(Consumer<ActionListener<Void>> indexer) {
        ActionListener<Void> onStarted = ActionListener.wrap(ignored -> {
            started = true;
            if (cancelled) {
                finishLocally();
                return;
            }
            logger.debug("downsample shard task [{}] of project [{}]: run [{}] started at epoch [{}]", docId, projectId, runId, epoch);
            indexer.accept(ActionListener.wrap(v -> finish(null), this::finish));
        }, e -> {
            logger.debug(() -> Strings.format("downsample shard task [%s] of project [%s]: could not start", docId, projectId), e);
            finishLocally();
        });
        if (claim == false) {
            enqueue(doc -> doc.running(runId, ShardTaskDoc.progressOf(progress)), WriteKind.START, onStarted);
            return;
        }
        final Versioned<ShardTaskDoc> unassigned;
        synchronized (this) {
            assert writing == false : "the claim of shard task [" + docId + "] must be the first write";
            writing = true;
            unassigned = current;
        }
        // no retry on a conflict: the task changed since it was found, most likely another node claimed it first
        store.claimShardTask(unassigned, runId, ShardTaskDoc.progressOf(progress), ActionListener.runAfter(ActionListener.wrap(written -> {
            synchronized (this) {
                current = written;
            }
            onStarted.onResponse(null);
        }, onStarted::onFailure), this::writeDone));
    }

    /**
     * Stops the run without writing anything, e.g. because the task was taken away from this node. The indexer stops at its next
     * cancellation check.
     */
    void cancel() {
        if (cancelled == false) {
            logger.debug("downsample shard task [{}] of project [{}]: run [{}] cancelled", docId, projectId, runId);
        }
        cancelled = true;
    }

    // ---- ShardTaskReporter ----

    @Override
    public DownsampleShardProgress progress() {
        return progress;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public String id() {
        return docId + "/" + runId;
    }

    @Override
    public void publishStatus(DownsampleShardIndexerStatus status) {
        // the shard task document gets the outcome when the run finishes, the status is part of the progress meanwhile
        progress.setDownsampleShardIndexerStatus(status);
    }

    @Override
    public void onBulkCompleted() {
        enqueue(doc -> doc.withProgress(ShardTaskDoc.progressOf(progress)), WriteKind.PROGRESS, ActionListener.noop());
    }

    // ---- outcome ----

    private void finish(Exception failure) {
        if (cancelled) {
            finishLocally();
            return;
        }
        final UnaryOperator<ShardTaskDoc> change;
        if (failure == null) {
            change = doc -> doc.completed(ShardTaskDoc.progressOf(progress));
        } else {
            boolean retriable = isRetriable(failure);
            Throwable cause = ExceptionsHelper.unwrapCause(failure);
            String error = cause.getClass().getSimpleName() + ": " + cause.getMessage();
            logger.debug(
                () -> Strings.format("downsample shard task [%s] of project [%s] failed (retriable: %s)", docId, projectId, retriable),
                failure
            );
            change = doc -> doc.failedOnNode(error, retriable, ShardTaskDoc.progressOf(progress));
        }
        enqueue(change, WriteKind.FINAL, ActionListener.running(this::finishLocally));
    }

    private static boolean isRetriable(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof DownsampleShardIndexerException indexerException) {
            return indexerException.isRetriable();
        }
        // the downsample thread pool is full: nothing wrong with the shard
        return cause instanceof EsRejectedExecutionException;
    }

    private void finishLocally() {
        if (done.compareAndSet(false, true)) {
            onDone.accept(this);
        }
    }

    // ---- serialized writes ----

    private enum WriteKind {
        /** marks a task this node already owns running, before the indexer starts, so nothing else is in flight */
        START,
        /** coalesced, and dropped once the run is finishing or cancelled */
        PROGRESS,
        /** the outcome, waits for the write in flight if any, never dropped */
        FINAL
    }

    private void enqueue(UnaryOperator<ShardTaskDoc> change, WriteKind kind, ActionListener<Void> listener) {
        synchronized (this) {
            switch (kind) {
                case START -> {
                    assert writing == false : "the start write of shard task [" + docId + "] must be the first one";
                }
                case PROGRESS -> {
                    if (finishing || cancelled) {
                        return;
                    }
                    if (writing) {
                        progressPending = true;
                        return;
                    }
                }
                case FINAL -> {
                    assert finishing == false : "two final writes for shard task [" + docId + "]";
                    finishing = true;
                    progressPending = false;
                    if (writing) {
                        finalWrite = new PendingWrite(change, listener);
                        return;
                    }
                }
            }
            writing = true;
        }
        write(change, ActionListener.runAfter(listener, this::writeDone), 0);
    }

    private void writeDone() {
        final PendingWrite next;
        synchronized (this) {
            if (finalWrite != null) {
                next = finalWrite;
                finalWrite = null;
            } else if (progressPending && finishing == false && cancelled == false) {
                progressPending = false;
                next = new PendingWrite(doc -> doc.withProgress(ShardTaskDoc.progressOf(progress)), ActionListener.noop());
            } else {
                writing = false;
                return;
            }
        }
        write(next.change(), ActionListener.runAfter(next.listener(), this::writeDone), 0);
    }

    private void write(UnaryOperator<ShardTaskDoc> change, ActionListener<Void> listener, int conflicts) {
        final Versioned<ShardTaskDoc> base;
        synchronized (this) {
            base = current;
        }
        store.updateShardTask(base, change.apply(base.doc()), epoch, ActionListener.wrap(written -> {
            synchronized (this) {
                current = written;
            }
            listener.onResponse(null);
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException && conflicts < MAX_CONFLICT_RETRIES) {
                rereadAndRetry(change, listener, conflicts, e);
            } else {
                if (ExceptionsHelper.unwrapCause(e) instanceof IllegalStateException) {
                    // the store refused the write: the task is no longer ours, or its target index is gone
                    cancel();
                }
                logger.debug(() -> Strings.format("downsample shard task [%s] of project [%s]: write failed", docId, projectId), e);
                listener.onFailure(e);
            }
        }));
    }

    private void rereadAndRetry(UnaryOperator<ShardTaskDoc> change, ActionListener<Void> listener, int conflicts, Exception conflict) {
        store.getShardTask(projectId, docId, ActionListener.wrap(found -> {
            ShardTaskDoc doc = found.map(Versioned::doc).orElse(null);
            if (doc != null && store.nodeId().equals(doc.assignedNode()) && doc.epoch() == epoch && doc.status().isOwnedByNode()) {
                synchronized (this) {
                    current = found.get();
                }
                write(change, listener, conflicts + 1);
            } else {
                // released, claimed by another node, failed by the master, or deleted
                cancel();
                listener.onFailure(conflict);
            }
        }, listener::onFailure));
    }
}
