/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.downsample.DownsampleShardTaskParams;
import org.elasticsearch.xpack.downsample.DownsampleSteps;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Drives the downsample operations recorded in {@link DownsampleStateIndex} on the elected master, without anything outside
 * having to call it again. Each operation is a state machine over {@link IndexTaskPhase}: every phase is idempotent and its
 * outcome is persisted before the next one starts, so a new master continues where the previous one stopped.
 * <p>
 * The master only orchestrates: it creates the target index, creates and (re)assigns one shard task per source shard, and
 * finalizes the target index once every shard task completed. The shard work runs on the nodes that hold the source shards.
 * <p>
 * Work is triggered when this node becomes master, when nodes leave or the routing changes, periodically every
 * {@link #RECONCILE_INTERVAL_SETTING}, and when a retry is due. At most one reconciliation per operation runs at a time.
 */
public class DownsampleOrchestrator implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DownsampleOrchestrator.class);

    /**
     * How often the master checks all active operations. It bounds how long the master takes to notice that the shard tasks
     * of an operation completed, since the nodes do not notify it.
     */
    public static final Setting<TimeValue> RECONCILE_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.downsample.store.reconcile_interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    /**
     * How many times a phase of an operation may fail with a retriable error before the operation fails.
     */
    static final int MAX_INDEX_TASK_ATTEMPTS = 10;
    /**
     * How many times a shard task may fail with a retriable error before the operation fails.
     */
    static final int MAX_SHARD_TASK_ATTEMPTS = 5;
    /**
     * The delay before the first retry of a failed phase; it doubles with every attempt, up to {@link #MAX_RETRY_DELAY}.
     */
    public static final Setting<TimeValue> RETRY_DELAY_SETTING = Setting.timeSetting(
        "xpack.downsample.store.retry_delay",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );
    static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueMinutes(5);
    private static final TimeValue MASTER_NODE_TIMEOUT = TimeValue.THIRTY_SECONDS;

    private final DownsampleStateStore store;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TimeValue reconcileInterval;
    private final TimeValue initialRetryDelay;
    private final ProjectResolver projectResolver;
    private final SetOnce<DownsampleSteps> steps = new SetOnce<>();
    private final Set<Operation> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<Operation> dirty = ConcurrentHashMap.newKeySet();
    /**
     * The downsample requests waiting on this master for their operation to finish.
     */
    private final Map<Operation, List<ActionListener<IndexTaskDoc>>> waiters = new ConcurrentHashMap<>();
    /**
     * The source index of every operation this master knows is active, for {@link #activeSourceIndices}.
     */
    private final Map<Operation, Index> activeSources = new ConcurrentHashMap<>();
    private volatile boolean isMaster;
    private volatile Scheduler.Cancellable periodicReconcile;

    /**
     * Identifies an operation: target index names are only unique within a project.
     */
    private record Operation(ProjectId projectId, String targetIndex) {}

    public DownsampleOrchestrator(
        DownsampleStateStore store,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        Settings settings
    ) {
        this.store = store;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.reconcileInterval = RECONCILE_INTERVAL_SETTING.get(settings);
        this.initialRetryDelay = RETRY_DELAY_SETTING.get(settings);
    }

    /**
     * Whether this node is the elected master, as far as this orchestrator knows (it follows the applied cluster states).
     */
    public boolean isMaster() {
        return isMaster;
    }

    /**
     * Called once, by the transport action that has the services the steps need.
     */
    public void setSteps(DownsampleSteps downsampleSteps) {
        steps.set(downsampleSteps);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            if (isMaster == false) {
                isMaster = true;
                periodicReconcile = threadPool.scheduleWithFixedDelay(this::reconcileAll, reconcileInterval, threadPool.generic());
                reconcileAll();
            } else if (event.nodesRemoved() || event.routingTableChanged()) {
                reconcileAll();
            }
        } else if (isMaster) {
            isMaster = false;
            Scheduler.Cancellable scheduled = periodicReconcile;
            if (scheduled != null) {
                scheduled.cancel();
            }
            activeSources.clear();
            // the requests waiting here are retried on the new master, where they join the same operations
            for (Operation operation : waiters.keySet()) {
                List<ActionListener<IndexTaskDoc>> waiting = waiters.remove(operation);
                if (waiting != null) {
                    waiting.forEach(
                        w -> w.onFailure(new NotMasterException("no longer the master, the downsample continues on the new one"))
                    );
                }
            }
        }
    }

    /**
     * Records a new downsample operation, or joins the one in progress for the same target index, triggers it, and notifies the
     * listener with the operation once it is finished ({@code DONE} or {@code FAILED}).
     * <p>
     * Fails with an {@link ElasticsearchTimeoutException} after {@code timeout}, the operation continues in the background. Fails
     * with a {@link NotMasterException} if this node is or stops being the master: the master node action retries on the new
     * master, where it joins the same operation, since recording an operation is idempotent.
     */
    public void submit(IndexTaskDoc doc, TimeValue timeout, ActionListener<IndexTaskDoc> originalListener) {
        // The listener completes on whichever thread finishes the operation; restore the caller's thread context for it, which holds
        // e.g. the response headers of the REST request (the product header, deprecation warnings).
        final ActionListener<IndexTaskDoc> listener = ContextPreservingActionListener.wrapPreservingContext(
            originalListener,
            threadPool.getThreadContext()
        );
        if (isMaster == false) {
            listener.onFailure(new NotMasterException("only the elected master can run downsample operations"));
            return;
        }
        final Operation operation = new Operation(doc.projectId(), doc.targetIndex());
        final AtomicReference<Scheduler.ScheduledCancellable> timeoutRef = new AtomicReference<>();
        final ActionListener<IndexTaskDoc> waiter = ActionListener.notifyOnce(ActionListener.runBefore(listener, () -> {
            Scheduler.ScheduledCancellable scheduled = timeoutRef.get();
            if (scheduled != null) {
                scheduled.cancel();
            }
        }));
        waiters.compute(operation, (key, waiting) -> {
            List<ActionListener<IndexTaskDoc>> list = waiting == null ? new ArrayList<>() : waiting;
            list.add(waiter);
            return list;
        });
        timeoutRef.set(threadPool.schedule(() -> {
            removeWaiter(operation, waiter);
            waiter.onFailure(
                new ElasticsearchTimeoutException(
                    "downsample [" + doc.targetIndex() + "] did not complete within [" + timeout + "], it continues in the background"
                )
            );
        }, timeout, threadPool.generic()));
        store.forMaster().createIndexTask(doc, ActionListener.wrap(recorded -> {
            if (recorded.doc().phase().isTerminal()) {
                completeWaiters(recorded.doc());
            } else {
                reconcile(recorded);
            }
        }, e -> {
            removeWaiter(operation, waiter);
            waiter.onFailure(e);
        }));
    }

    /**
     * The source indices of a project that are being downsampled through the system index, as far as this master knows: the
     * operations it recorded or reconciled and that are not finished. Right after a master failover it is empty until the first
     * reconciliation of the new master (at most {@link #RECONCILE_INTERVAL_SETTING}).
     * <p>
     * Synchronous on purpose, for {@link org.elasticsearch.datastreams.DownsamplingOperations}, which the data stream lifecycle uses to
     * limit the number of concurrent downsample operations.
     */
    public Set<Index> activeSourceIndices(ProjectId projectId) {
        Set<Index> sources = new HashSet<>();
        activeSources.forEach((operation, source) -> {
            if (operation.projectId().equals(projectId)) {
                sources.add(source);
            }
        });
        return sources;
    }

    private void track(IndexTaskDoc doc) {
        Operation operation = new Operation(doc.projectId(), doc.targetIndex());
        if (doc.phase().isTerminal()) {
            activeSources.remove(operation);
        } else {
            activeSources.put(operation, new Index(doc.sourceIndex(), doc.sourceIndexUuid()));
        }
    }

    private void removeWaiter(Operation operation, ActionListener<IndexTaskDoc> waiter) {
        waiters.computeIfPresent(operation, (key, waiting) -> {
            waiting.remove(waiter);
            return waiting.isEmpty() ? null : waiting;
        });
    }

    private void completeWaiters(IndexTaskDoc finished) {
        assert finished.phase().isTerminal() : finished.phase();
        track(finished);
        List<ActionListener<IndexTaskDoc>> waiting = waiters.remove(new Operation(finished.projectId(), finished.targetIndex()));
        if (waiting != null) {
            waiting.forEach(w -> w.onResponse(finished));
        }
    }

    /**
     * Reconciles every active operation, of every project.
     */
    public void reconcileAll() {
        if (isMaster == false || steps.get() == null) {
            return;
        }
        // one search per project that ever had an operation; the others have no state index
        List<ProjectId> projects = DownsampleStateStore.projectsWithStateIndex(clusterService.state());
        if (projects.isEmpty()) {
            return;
        }
        threadPool.generic().execute(() -> {
            for (ProjectId projectId : projects) {
                store.forMaster()
                    .findActiveIndexTasks(
                        projectId,
                        ActionListener.wrap(
                            // The search is near real time: it only finds the candidates. Each one is read again in real time
                            // before a phase runs on it, otherwise a stale copy (e.g. still in a phase it has already left) would
                            // run that phase again; its write would be rejected, but not the side effects of the phase.
                            tasks -> tasks.forEach(task -> reconcile(task.doc().projectId(), task.doc().targetIndex())),
                            // e.g. the state index was just created and its shard is not started yet: the next sweep retries
                            e -> logger.debug(
                                () -> Strings.format("failed to load the active downsample operations of project [%s]", projectId),
                                e
                            )
                        )
                    );
            }
        });
    }

    /**
     * Reconciles one operation, e.g. right after it was requested.
     */
    public void reconcile(ProjectId projectId, String targetIndex) {
        if (isMaster == false) {
            return;
        }
        store.forMaster()
            .getIndexTask(
                projectId,
                targetIndex,
                ActionListener.wrap(
                    task -> task.ifPresent(this::reconcile),
                    e -> logger.warn(
                        () -> Strings.format("failed to load downsample operation [%s] of project [%s]", targetIndex, projectId),
                        e
                    )
                )
            );
    }

    /**
     * @param task a real time read or the result of a write, never a search hit: a phase runs on it
     */
    private void reconcile(Versioned<IndexTaskDoc> task) {
        track(task.doc());
        final Operation operation = new Operation(task.doc().projectId(), task.doc().targetIndex());
        if (inFlight.add(operation) == false) {
            // picked up by the running reconciliation when it completes (or, in a rare race, by the next periodic one)
            dirty.add(operation);
            return;
        }
        ActionListener<Void> done = ActionListener.running(() -> {
            inFlight.remove(operation);
            if (dirty.remove(operation)) {
                reconcile(operation.projectId(), operation.targetIndex());
            }
        });
        ActionListener.run(done, l -> runPhase(task, l));
    }

    private void runPhase(Versioned<IndexTaskDoc> task, ActionListener<Void> done) {
        final IndexTaskDoc doc = task.doc();
        if (isMaster == false) {
            done.onResponse(null);
            return;
        }
        if (doc.phase().isTerminal()) {
            // e.g. a request joined the operation just before it finished
            completeWaiters(doc);
            done.onResponse(null);
            return;
        }
        final long now = threadPool.absoluteTimeInMillis();
        if (doc.nextRetryAt() > now) {
            scheduleReconcile(doc.projectId(), doc.targetIndex(), doc.nextRetryAt() - now);
            done.onResponse(null);
            return;
        }
        if (now > doc.deadline() && doc.phase().ordinal() < IndexTaskPhase.MARK_SUCCESS.ordinal()) {
            write(task, doc.failed("the downsample operation did not complete before its deadline"), done);
            return;
        }
        ActionListener<IndexTaskDoc> phaseListener = ActionListener.wrap(next -> {
            if (next == null) {
                done.onResponse(null);
            } else {
                write(task, next, done);
            }
        }, e -> onPhaseFailure(task, e, done));
        ActionListener.run(phaseListener, l -> {
            logger.debug("downsample [{}]: running phase [{}]", doc.targetIndex(), doc.phase());
            switch (doc.phase()) {
                case CREATE_TARGET -> createTarget(doc, l);
                case RUN_SHARDS -> runShards(doc, l);
                case FINALIZE_SETTINGS -> finalizeSettings(doc, l);
                case REFRESH -> refresh(doc, l);
                case MARK_SUCCESS -> markSuccess(doc, l);
                case FLUSH -> flush(doc, l);
                case DONE, FAILED -> throw new IllegalStateException("terminal phase [" + doc.phase() + "] cannot run");
            }
        });
    }

    /**
     * Persists the result of a phase, then continues right away with the next phase.
     */
    private void write(Versioned<IndexTaskDoc> current, IndexTaskDoc next, ActionListener<Void> done) {
        final String targetIndex = next.targetIndex();
        store.forMaster().updateIndexTask(current, next, ActionListener.wrap(written -> {
            IndexTaskDoc doc = written.doc();
            track(doc);
            logger.debug("downsample [{}]: phase [{}] attempt [{}]", targetIndex, doc.phase(), doc.attempt());
            if (doc.phase() == IndexTaskPhase.FAILED) {
                logger.warn("downsample [{}] of project [{}] failed: [{}]", targetIndex, doc.projectId(), doc.lastError());
                completeWaiters(doc);
                done.onResponse(null);
            } else if (doc.phase().isTerminal()) {
                logger.info("downsample [{}] of project [{}] completed", targetIndex, doc.projectId());
                completeWaiters(doc);
                done.onResponse(null);
            } else {
                runPhase(written, done);
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                // someone else (e.g. a previous master) changed the operation: re-read it and decide again
                dirty.add(new Operation(next.projectId(), targetIndex));
            } else {
                logger.warn(() -> Strings.format("failed to update downsample operation [%s]", targetIndex), e);
            }
            done.onResponse(null);
        }));
    }

    private void onPhaseFailure(Versioned<IndexTaskDoc> task, Exception e, ActionListener<Void> done) {
        final IndexTaskDoc doc = task.doc();
        final String error = describe(e);
        if (isRetriable(e) && doc.attempt() + 1 < MAX_INDEX_TASK_ATTEMPTS) {
            TimeValue delay = retryDelay(initialRetryDelay, doc.attempt());
            logger.debug(
                () -> Strings.format("downsample [%s]: phase [%s] failed, retrying in [%s]", doc.targetIndex(), doc.phase(), delay),
                e
            );
            write(task, doc.withRetry(error, threadPool.absoluteTimeInMillis() + delay.millis()), done);
        } else {
            logger.warn(
                () -> Strings.format("downsample [%s] of project [%s]: phase [%s] failed", doc.targetIndex(), doc.projectId(), doc.phase()),
                e
            );
            write(task, doc.failed(error), done);
        }
    }

    // ---- phases, each completes the listener with the next version of the document, or null to wait ----

    private void createTarget(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        final ClusterState state = clusterService.state();
        final ProjectMetadata project = project(state, doc);
        final IndexMetadata source = sourceIndex(project, doc);
        inProject(doc, listener.<DownsampleSteps.Prepared>delegateFailureAndWrap((l, prepared) -> {
            IndexMetadata existing = project(doc).index(doc.targetIndex());
            if (existing != null) {
                // created by an earlier attempt of this operation (or not, which createShardTasks checks)
                createShardTasks(doc, source, existing, prepared, l);
                return;
            }
            steps().createTargetIndex(
                project.id(),
                doc.targetIndex(),
                steps().minNumReplicas(),
                source,
                prepared.targetMapping(),
                doc.config(),
                MASTER_NODE_TIMEOUT,
                ActionListener.wrap(ack -> {
                    IndexMetadata created = project(doc).index(doc.targetIndex());
                    if (created == null) {
                        throw new ElasticsearchException("target index [" + doc.targetIndex() + "] was not created");
                    }
                    createShardTasks(doc, source, created, prepared, l);
                }, e -> {
                    IndexMetadata raced = project(doc).index(doc.targetIndex());
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException && raced != null) {
                        createShardTasks(doc, source, raced, prepared, l);
                    } else {
                        l.onFailure(e);
                    }
                })
            );
        }), l -> steps().prepare(source, doc.config(), state, MASTER_NODE_TIMEOUT, null, l));
    }

    private void createShardTasks(
        IndexTaskDoc doc,
        IndexMetadata source,
        IndexMetadata target,
        DownsampleSteps.Prepared prepared,
        ActionListener<IndexTaskDoc> listener
    ) {
        if (IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()) == DownsampleTaskStatus.UNKNOWN) {
            throw new NonRetriableException("target index [" + doc.targetIndex() + "] already exists and is not a downsample index");
        }
        String downsampledFrom = IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.get(target.getSettings());
        if (doc.sourceIndexUuid().equals(downsampledFrom) == false) {
            throw new NonRetriableException(
                "target index [" + doc.targetIndex() + "] already exists and is a downsample of another index [" + downsampledFrom + "]"
            );
        }
        final String targetUuid = target.getIndexUUID();
        try (RefCountingListener refs = new RefCountingListener(listener.map(ignored -> doc.targetCreated(targetUuid)))) {
            for (int shard = 0; shard < source.getNumberOfShards(); shard++) {
                DownsampleShardTaskParams params = DownsampleSteps.shardTaskParams(
                    doc.config(),
                    source,
                    doc.targetIndex(),
                    prepared.fields(),
                    new ShardId(source.getIndex(), shard)
                );
                store.forMaster()
                    .createShardTask(
                        ShardTaskDoc.newTask(doc.projectId(), doc.targetIndex(), targetUuid, params),
                        refs.acquire(ignored -> {})
                    );
            }
        }
    }

    private void runShards(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        final ProjectMetadata project = project(doc);
        final IndexMetadata target = targetIndex(project, doc);
        if (IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(target.getSettings()) == DownsampleTaskStatus.SUCCESS) {
            // completed by other means (e.g. the persistent task path), nothing left to do
            listener.onResponse(doc.withPhase(IndexTaskPhase.DONE));
            return;
        }
        final IndexMetadata source = sourceIndex(project, doc);
        final int numberOfShards = source.getNumberOfShards();
        store.forMaster()
            .getShardTasks(
                doc.projectId(),
                doc.targetIndex(),
                doc.targetIndexUuid(),
                numberOfShards,
                listener.delegateFailureAndWrap((l, found) -> {
                    List<Versioned<ShardTaskDoc>> tasks = new ArrayList<>(numberOfShards);
                    for (int shard = 0; shard < numberOfShards; shard++) {
                        Optional<Versioned<ShardTaskDoc>> task = found.get(shard);
                        if (task.isEmpty()) {
                            throw new ElasticsearchException(
                                "shard task [" + shard + "] of downsample [" + doc.targetIndex() + "] is missing"
                            );
                        }
                        tasks.add(task.get());
                    }
                    planShardTasks(doc, tasks, l);
                })
            );
    }

    private void planShardTasks(IndexTaskDoc doc, List<Versioned<ShardTaskDoc>> tasks, ActionListener<IndexTaskDoc> listener) {
        final ClusterState state = clusterService.state();
        final Set<String> nodes = state.nodes().getNodes().keySet();
        final Set<String> shuttingDown = state.metadata().nodeShutdowns().getAllNodeIds();
        final IndexRoutingTable sourceRouting = state.routingTable(doc.projectId()).index(tasks.get(0).doc().sourceShardId().getIndex());
        final Map<String, Integer> load = ShardTaskPlanner.load(tasks.stream().map(Versioned::doc).toList());
        final List<ShardTaskDoc> planned = new ArrayList<>(tasks.size());
        final List<Runnable> writes = new ArrayList<>();
        final AtomicBoolean writeFailed = new AtomicBoolean();
        try (RefCountingListener refs = new RefCountingListener(listener.map(ignored -> {
            if (writeFailed.get()) {
                // the documents changed under us or could not be written, decide again with fresh ones
                return null;
            }
            return switch (ShardTaskPlanner.outcome(planned)) {
                case ALL_COMPLETED -> doc.withPhase(IndexTaskPhase.FINALIZE_SETTINGS);
                case IN_PROGRESS -> null;
                case FAILED -> throw new NonRetriableException(
                    "downsampling failed for shard(s) "
                        + planned.stream()
                            .filter(t -> t.status() == ShardTaskStatus.FAILED)
                            .map(t -> "[" + t.shard() + "]: " + t.lastError())
                            .toList()
                );
            };
        }))) {
            for (Versioned<ShardTaskDoc> task : tasks) {
                ShardTaskDoc current = task.doc();
                List<String> candidates = ShardTaskPlanner.candidateNodes(
                    sourceRouting == null ? null : sourceRouting.shard(current.shard()),
                    load,
                    shuttingDown
                );
                ShardTaskDoc next = ShardTaskPlanner.plan(current, nodes, candidates, MAX_SHARD_TASK_ATTEMPTS);
                if (next == null) {
                    planned.add(current);
                    continue;
                }
                if (current.status().isOwnedByNode()) {
                    load.merge(current.assignedNode(), -1, Integer::sum);
                }
                if (next.status().isOwnedByNode()) {
                    load.merge(next.assignedNode(), 1, Integer::sum);
                }
                planned.add(next);
                ActionListener<Void> writeRef = refs.acquire();
                writes.add(() -> store.forMaster().updateShardTask(task, next, ActionListener.wrap(written -> {
                    logger.debug(
                        "downsample [{}]: shard task [{}] is [{}] on [{}] at epoch [{}]",
                        doc.targetIndex(),
                        next.shard(),
                        next.status(),
                        next.assignedNode(),
                        next.epoch()
                    );
                    writeRef.onResponse(null);
                }, e -> {
                    writeFailed.set(true);
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        dirty.add(new Operation(doc.projectId(), doc.targetIndex()));
                    } else {
                        logger.warn(() -> Strings.format("failed to update shard task [%s]", next.id()), e);
                    }
                    // the outcome is decided from the planned documents only if every write succeeded
                    writeRef.onResponse(null);
                })));
            }
            writes.forEach(Runnable::run);
        }
    }

    private void finalizeSettings(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        final ProjectMetadata project = project(doc);
        targetIndex(project, doc);
        final IndexMetadata source = sourceIndex(project, doc);
        inProject(
            doc,
            listener.<AcknowledgedResponse>map(ignored -> doc.withPhase(IndexTaskPhase.REFRESH)),
            l -> steps().updateTargetSettings(doc.targetIndex(), steps().minNumReplicas(), source, null, l)
        );
    }

    private void refresh(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        targetIndex(project(doc), doc);
        inProject(doc, listener.<BroadcastResponse>map(response -> {
            if (response.getFailedShards() != 0) {
                logger.info(
                    "downsample [{}] of project [{}]: refresh failed on [{}] shard(s)",
                    doc.targetIndex(),
                    doc.projectId(),
                    response.getFailedShards()
                );
            }
            return doc.withPhase(IndexTaskPhase.MARK_SUCCESS);
        }), l -> steps().refresh(doc.targetIndex(), null, l));
    }

    private void markSuccess(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        targetIndex(project(doc), doc);
        // a cluster state update, which names its project explicitly
        steps().markSuccess(
            doc.projectId(),
            doc.targetIndex(),
            MASTER_NODE_TIMEOUT,
            listener.map(ignored -> doc.withPhase(IndexTaskPhase.FLUSH))
        );
    }

    private void flush(IndexTaskDoc doc, ActionListener<IndexTaskDoc> listener) {
        targetIndex(project(doc), doc);
        inProject(doc, listener.<Void>map(ignored -> doc.withPhase(IndexTaskPhase.DONE)), l -> steps().flush(doc.targetIndex(), null, l));
    }

    // ---- helpers ----

    /**
     * The source index of the operation, failing for good if it was deleted or replaced by another index with the same name.
     */
    private static IndexMetadata sourceIndex(ProjectMetadata project, IndexTaskDoc doc) {
        IndexMetadata source = project.index(doc.sourceIndex());
        if (source == null || source.getIndexUUID().equals(doc.sourceIndexUuid()) == false) {
            throw new NonRetriableException(
                "source index [" + doc.sourceIndex() + "] with uuid [" + doc.sourceIndexUuid() + "] no longer exists"
            );
        }
        return source;
    }

    /**
     * The target index of the operation, failing for good if it was deleted or replaced by another index with the same name.
     */
    private static IndexMetadata targetIndex(ProjectMetadata project, IndexTaskDoc doc) {
        IndexMetadata target = project.index(doc.targetIndex());
        if (target == null || target.getIndexUUID().equals(doc.targetIndexUuid()) == false) {
            throw new NonRetriableException(
                "target index [" + doc.targetIndex() + "] with uuid [" + doc.targetIndexUuid() + "] no longer exists"
            );
        }
        return target;
    }

    /**
     * The project of the operation in the current cluster state, failing for good if it was deleted.
     */
    private ProjectMetadata project(IndexTaskDoc doc) {
        return project(clusterService.state(), doc);
    }

    private static ProjectMetadata project(ClusterState state, IndexTaskDoc doc) {
        ProjectMetadata project = state.metadata().projects().get(doc.projectId());
        if (project == null) {
            throw new NonRetriableException("project [" + doc.projectId() + "] no longer exists");
        }
        return project;
    }

    /**
     * Sends requests in the project of the operation. Cluster state updates (create the target index, mark it successful) name
     * their project explicitly and do not need this.
     */
    private <T> void inProject(IndexTaskDoc doc, ActionListener<T> listener, Consumer<ActionListener<T>> request) {
        ProjectContext.run(threadPool.getThreadContext(), projectResolver, doc.projectId(), listener, request);
    }

    private DownsampleSteps steps() {
        DownsampleSteps downsampleSteps = steps.get();
        if (downsampleSteps == null) {
            throw new IllegalStateException("the downsample orchestrator is not initialized");
        }
        return downsampleSteps;
    }

    private void scheduleReconcile(ProjectId projectId, String targetIndex, long delayMillis) {
        threadPool.schedule(() -> reconcile(projectId, targetIndex), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
    }

    static TimeValue retryDelay(TimeValue initialDelay, int attempt) {
        long delay = initialDelay.millis() << Math.min(attempt, 20);
        return TimeValue.timeValueMillis(Math.min(delay, MAX_RETRY_DELAY.millis()));
    }

    private static boolean isRetriable(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return (cause instanceof NonRetriableException
            || cause instanceof ActionRequestValidationException
            || cause instanceof IllegalArgumentException) == false;
    }

    private static String describe(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /**
     * A failure that retrying cannot fix, e.g. the source index is gone.
     */
    static class NonRetriableException extends ElasticsearchException {
        NonRetriableException(String message) {
            super(message);
        }
    }

}
