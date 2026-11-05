/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.downsample.DownsampleShardPersistentTaskExecutor.DelegatingAction;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Claims and runs shard tasks on a data node. Every {@link #POLL_INTERVAL_SETTING}, and as soon as a slot frees up, it goes
 * through the state index of every project that has one:
 * <ol>
 *     <li>it reconciles the tasks this node owns with its local runs: a task {@code RUNNING} on this node without a local run is
 *     resumed (e.g. after this node restarted), a local run whose task is no longer owned by this node at the same epoch is
 *     cancelled. The search is near real time, so a run is only cancelled after a real time read confirms it, never on a missing
 *     search hit alone;</li>
 *     <li>with the slots left, it claims unassigned tasks of the source shards it holds a searchable copy of, oldest operation
 *     first, so the operations requested first finish first. A node marked for shutdown claims nothing. Nodes race for a task
 *     with a compare-and-set, the loser skips it.</li>
 * </ol>
 * A node only takes the work it can start right away, so a task waits unassigned, claimable by any node with a copy, rather
 * than queued behind a busy node.
 * <p>
 * There is no handshake with the master and no transport action: the documents are the only interface. The shard work runs
 * through {@link DelegatingAction} in a system context, like the persistent task path, so as the same internal user.
 */
public class ShardTaskPoller extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ShardTaskPoller.class);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.downsample.store.poll_interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private record RunKey(ProjectId projectId, String docId) {}

    private final Client client;
    private final DownsampleStateStore store;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final boolean enabled;
    private final BiConsumer<ShardTaskRun, ActionListener<Void>> indexer;
    private final int maxConcurrentRuns;
    /**
     * Set when a task was left waiting because this node runs as many shard tasks as it can: the next run that finishes polls
     * right away instead of waiting for the next interval, so a free slot does not stay idle.
     */
    private final AtomicBoolean waitingForSlot = new AtomicBoolean();
    private final Map<RunKey, ShardTaskRun> runs = new ConcurrentHashMap<>();
    private volatile TimeValue pollInterval;
    private volatile Scheduler.Cancellable scheduledPoll;

    /**
     * @param client  a client without origin: the shard work must run in the system context, see {@link ProjectContext#runAsSystem}
     * @param enabled whether this node runs shard tasks (the feature is enabled and the node can hold data)
     */
    public ShardTaskPoller(
        Client client,
        DownsampleStateStore store,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        boolean enabled
    ) {
        this(client, store, clusterService, threadPool, projectResolver, enabled, null, 0);
    }

    /**
     * @param indexer           runs the indexer of a shard task and notifies the listener when it is done; {@code null} for the
     *                          real one. Tests supply their own to control when a run completes.
     * @param maxConcurrentRuns how many shard tasks this node runs at the same time; 0 for the size of the downsample thread pool
     */
    ShardTaskPoller(
        Client client,
        DownsampleStateStore store,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        boolean enabled,
        @Nullable BiConsumer<ShardTaskRun, ActionListener<Void>> indexer,
        int maxConcurrentRuns
    ) {
        this.client = client;
        this.indexer = indexer == null ? this::runIndexer : indexer;
        // Every run takes a thread of the downsample pool for its whole duration. Starting more would only queue them there, and a
        // rejection from a full queue counts as a failed attempt of the shard task, so the node claims no more than this.
        this.maxConcurrentRuns = maxConcurrentRuns > 0
            ? maxConcurrentRuns
            : threadPool.info(Downsample.DOWNSAMPLE_TASK_THREAD_POOL_NAME).getMax();
        this.store = store;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.enabled = enabled;
        this.pollInterval = POLL_INTERVAL_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, interval -> this.pollInterval = interval);
    }

    @Override
    protected void doStart() {
        if (enabled) {
            scheduleNextPoll();
        }
    }

    @Override
    protected void doStop() {
        Scheduler.Cancellable scheduled = scheduledPoll;
        if (scheduled != null) {
            scheduled.cancel();
        }
        runs.values().forEach(ShardTaskRun::cancel);
    }

    @Override
    protected void doClose() {}

    private void scheduleNextPoll() {
        // not lifecycle.started(): doStart() runs before the lifecycle moves to started
        if (lifecycle.stoppedOrClosed() == false) {
            scheduledPoll = threadPool.schedule(
                () -> poll(ActionListener.running(this::scheduleNextPoll)),
                pollInterval,
                threadPool.generic()
            );
        }
    }

    /**
     * One round: reconcile the tasks this node owns in every project, then claim new ones with the free slots.
     */
    void poll(ActionListener<Void> listener) {
        final ClusterState state = clusterService.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || state.nodes().getLocalNode() == null) {
            listener.onResponse(null);
            return;
        }
        final List<ProjectId> projects = DownsampleStateStore.projectsWithStateIndex(state);
        // the state index of a deleted project is gone with it, and so are its tasks
        runs.forEach((key, run) -> {
            if (projects.contains(key.projectId()) == false) {
                run.cancel();
                runs.remove(key, run);
            }
        });
        final DownsampleStateStore.NodeView node = store.forLocalNode();
        try (RefCountingListener refs = new RefCountingListener(listener.delegateFailureAndWrap((l, v) -> claim(node, projects, l)))) {
            for (ProjectId projectId : projects) {
                ActionListener<Void> ref = refs.acquire();
                node.findMyShardTasks(projectId, ActionListener.wrap(found -> reconcile(node, projectId, found, ref), e -> {
                    logger.debug(() -> Strings.format("failed to find the downsample shard tasks of project [%s]", projectId), e);
                    ref.onResponse(null);
                }));
            }
        }
    }

    /**
     * Reconciles the local runs of a project with the shard tasks this node owns, as found by the (near real time) search, and
     * notifies the listener once the real time confirmations it needed are done.
     */
    void reconcile(
        DownsampleStateStore.NodeView node,
        ProjectId projectId,
        List<Versioned<ShardTaskDoc>> found,
        ActionListener<Void> done
    ) {
        Set<String> seen = new HashSet<>();
        for (Versioned<ShardTaskDoc> task : found) {
            ShardTaskDoc doc = task.doc();
            seen.add(doc.id());
            RunKey key = new RunKey(projectId, doc.id());
            ShardTaskRun existing = runs.get(key);
            if (existing != null && existing.epoch() >= doc.epoch()) {
                // the same epoch, or a stale hit from before the current run: keep the current run
                continue;
            }
            if (existing != null) {
                // the task was taken from this node and claimed by it again: the old run is fenced, resume with a new one
                existing.cancel();
                runs.remove(key, existing);
            }
            if (start(node, key, task) == false) {
                waitingForSlot.set(true);
            }
        }
        try (RefCountingListener refs = new RefCountingListener(done)) {
            runs.forEach((key, run) -> {
                // a run whose claim is in flight is not in the document yet, its own write decides its fate
                if (key.projectId().equals(projectId) && seen.contains(key.docId()) == false && run.hasStarted()) {
                    confirmAndCancel(node, key, run, refs.acquire());
                }
            });
        }
    }

    /**
     * Claims, with the slots left, the unassigned tasks this node can run, oldest operation first across all projects.
     */
    private void claim(DownsampleStateStore.NodeView node, List<ProjectId> projects, ActionListener<Void> listener) {
        final int freeSlots = maxConcurrentRuns - runs.size();
        if (freeSlots <= 0) {
            // there may be work to claim: look again as soon as a run finishes
            waitingForSlot.set(true);
            listener.onResponse(null);
            return;
        }
        final ClusterState state = clusterService.state();
        if (state.metadata().nodeShutdowns().contains(node.nodeId())) {
            listener.onResponse(null);
            return;
        }
        final Set<String> localIndices = DownsampleStateStore.locallySearchableIndexUuids(state, node.nodeId());
        if (localIndices.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final List<Versioned<ShardTaskDoc>> claimable = Collections.synchronizedList(new ArrayList<>());
        try (RefCountingListener refs = new RefCountingListener(listener.delegateFailureAndWrap((l, v) -> {
            claimInOrder(node, claimable);
            l.onResponse(null);
        }))) {
            for (ProjectId projectId : projects) {
                ActionListener<Void> ref = refs.acquire();
                // the search is near real time: ask for more than the free slots, so the hits of tasks this node claimed already
                // (they still look unassigned) do not hide the claimable ones
                node.findClaimableShardTasks(projectId, localIndices, freeSlots + maxConcurrentRuns, ActionListener.wrap(found -> {
                    claimable.addAll(found);
                    ref.onResponse(null);
                }, e -> {
                    logger.debug(() -> Strings.format("failed to find claimable downsample shard tasks of project [%s]", projectId), e);
                    ref.onResponse(null);
                }));
            }
        }
    }

    private void claimInOrder(DownsampleStateStore.NodeView node, List<Versioned<ShardTaskDoc>> claimable) {
        final ClusterState state = clusterService.state();
        final List<Versioned<ShardTaskDoc>> ordered;
        synchronized (claimable) {
            ordered = claimable.stream().sorted(OLDEST_FIRST).toList();
        }
        for (Versioned<ShardTaskDoc> task : ordered) {
            ShardTaskDoc doc = task.doc();
            if (runs.containsKey(new RunKey(doc.projectId(), doc.id()))) {
                continue;
            }
            String ineligible = DownsampleStateStore.whyCannotClaim(state, node.nodeId(), doc);
            if (ineligible != null) {
                logger.trace("not claiming downsample shard task [{}] of project [{}]: {}", doc.id(), doc.projectId(), ineligible);
                continue;
            }
            if (start(node, new RunKey(doc.projectId(), doc.id()), task) == false) {
                waitingForSlot.set(true);
                return;
            }
        }
    }

    private static final Comparator<Versioned<ShardTaskDoc>> OLDEST_FIRST = Comparator.<Versioned<ShardTaskDoc>>comparingLong(
        t -> t.doc().createdAt()
    ).thenComparing(t -> t.doc().projectId().id()).thenComparing(t -> t.doc().targetIndex()).thenComparingInt(t -> t.doc().shard());

    /**
     * Starts a run that claims or resumes the task, unless one runs already.
     *
     * @return {@code false} if there was no free slot for it
     */
    private boolean start(DownsampleStateStore.NodeView node, RunKey key, Versioned<ShardTaskDoc> task) {
        final ShardTaskRun run = new ShardTaskRun(node, task, this::runFinished);
        synchronized (runs) {
            // projects are reconciled concurrently: check the limit and take the slot atomically
            if (runs.containsKey(key)) {
                return true;
            }
            if (runs.size() >= maxConcurrentRuns) {
                logger.debug("[{}] shard tasks running, [{}] waits for a free slot", runs.size(), key.docId());
                return false;
            }
            runs.put(key, run);
        }
        run.start(completion -> indexer.accept(run, completion));
        return true;
    }

    private void runFinished(ShardTaskRun finished) {
        runs.remove(new RunKey(finished.projectId(), finished.docId()), finished);
        if (waitingForSlot.getAndSet(false) && lifecycle.stoppedOrClosed() == false) {
            threadPool.generic().execute(() -> poll(ActionListener.noop()));
        }
    }

    /**
     * Runs the indexer in the project of the task, in a system context, through {@link DelegatingAction} so security sets up the
     * index permissions the searcher needs (see there).
     */
    private void runIndexer(ShardTaskRun run, ActionListener<Void> completion) {
        ProjectContext.<ActionResponse.Empty>runAsSystem(
            threadPool.getThreadContext(),
            projectResolver,
            run.projectId(),
            // the action responds as soon as the indexer is forked; only a failure to get there is reported here
            ActionListener.wrap(ignored -> {}, completion::onFailure),
            l -> client.execute(DelegatingAction.INSTANCE, new DelegatingAction.Request(run, null, run.params(), completion), l)
        );
    }

    /**
     * A local run whose task was not found by the (near real time) search: cancel it only if a real time read confirms that the
     * task is no longer owned by this node at the run's epoch.
     */
    private void confirmAndCancel(DownsampleStateStore.NodeView node, RunKey key, ShardTaskRun run, ActionListener<Void> done) {
        node.getShardTask(key.projectId(), key.docId(), ActionListener.wrap(found -> {
            ShardTaskDoc doc = found.map(Versioned::doc).orElse(null);
            boolean stillOurs = doc != null
                && node.nodeId().equals(doc.assignedNode())
                && doc.epoch() == run.epoch()
                && doc.status().isOwnedByNode();
            if (stillOurs == false) {
                run.cancel();
                runs.remove(key, run);
            }
            done.onResponse(null);
        }, e -> {
            logger.debug(() -> Strings.format("failed to read downsample shard task [%s]", key.docId()), e);
            done.onResponse(null);
        }));
    }

    /**
     * The number of local runs, for tests.
     */
    public int activeRuns() {
        return runs.size();
    }
}
