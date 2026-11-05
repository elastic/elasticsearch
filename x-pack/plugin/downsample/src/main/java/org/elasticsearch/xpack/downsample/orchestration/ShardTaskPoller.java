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
import org.elasticsearch.xpack.downsample.DownsampleShardPersistentTaskExecutor.DelegatingAction;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Runs, on a data node, the shard tasks the master assigned to it. Every {@link #POLL_INTERVAL_SETTING} it searches the state
 * index of every project that has one for the shard tasks assigned to this node, and reconciles them with its local runs:
 * <ul>
 *     <li>a task assigned to this node without a local run is started, including after this node restarted (the task is still
 *     {@code RUNNING} on it);</li>
 *     <li>a local run whose task is no longer assigned to this node at the same epoch is cancelled. The search is near real time,
 *     so a run is only cancelled after a real time read confirms it, never on a missing search hit alone.</li>
 * </ul>
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
        this(client, store, clusterService, threadPool, projectResolver, enabled, null);
    }

    /**
     * @param indexer runs the indexer of a shard task and notifies the listener when it is done; {@code null} for the real one.
     *                Tests supply their own to control when a run completes.
     */
    ShardTaskPoller(
        Client client,
        DownsampleStateStore store,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        boolean enabled,
        @Nullable BiConsumer<ShardTaskRun, ActionListener<Void>> indexer
    ) {
        this.client = client;
        this.indexer = indexer == null ? this::runIndexer : indexer;
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
     * One round: find this node's shard tasks in every project and reconcile them with the local runs.
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
        try (RefCountingListener refs = new RefCountingListener(listener)) {
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
     * Reconciles the local runs of a project with the shard tasks found by the (near real time) search, and notifies the listener
     * once the real time confirmations it needed are done.
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
                // the task was taken from this node and given back to it: the old run is fenced, start a new one
                existing.cancel();
                runs.remove(key, existing);
            }
            start(node, key, task);
        }
        try (RefCountingListener refs = new RefCountingListener(done)) {
            runs.forEach((key, run) -> {
                if (key.projectId().equals(projectId) && seen.contains(key.docId()) == false) {
                    confirmAndCancel(node, key, run, refs.acquire());
                }
            });
        }
    }

    private void start(DownsampleStateStore.NodeView node, RunKey key, Versioned<ShardTaskDoc> task) {
        final ShardTaskRun run = new ShardTaskRun(node, task, finished -> runs.remove(key, finished));
        if (runs.putIfAbsent(key, run) != null) {
            return;
        }
        run.start(completion -> indexer.accept(run, completion));
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
     * task is no longer assigned to this node at the run's epoch.
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
