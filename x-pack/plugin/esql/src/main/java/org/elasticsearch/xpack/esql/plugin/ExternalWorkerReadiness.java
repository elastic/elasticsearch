/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Keeps a node that has just joined the cluster out of this coordinator's pool of external workers until that node has
 * applied the cluster state that added it.
 *
 * <p>A node accepts transport requests before it has applied any cluster state, and a coordinator sees it as an external
 * worker as soon as the coordinator itself applies the join. Until the node's own first apply completes it has no
 * datasets, no encryption key and default settings, so every external split sent to it in that window fails or waits
 * (elastic/esql-planning#2319). Shard requests have no such window: a shard is routable only once its node reported it
 * started. External splits are routed by role, so the readiness is established here instead: each added node is asked,
 * through {@link TransportAwaitClusterStateVersionAppliedAction}, to confirm it has applied the version that added it,
 * and is held back until it answers. The answer comes from the node's applier thread, so a node still in its first
 * apply answers only once that apply is done.
 *
 * <p>This covers the join. A node that is behind a later state, a dataset change the coordinator planned from, say, is
 * not held back; the worker-side wait in {@code DataNodeComputeHandler} covers that, bounded by
 * {@code esql.external.worker_ready_timeout}. When every worker there is has just joined, which can only be on a
 * coordinator that is not itself a worker (the local node is never held back), the query waits here for one of them
 * ({@link #awaitEligibleWorker}), bounded by the same setting, rather than scan on a coordinator that may be thin or
 * an index node. The await has no timeout of its own: a node that never applies the state
 * stays out of the pool, which is the point, and leaves it when it leaves the cluster, which the master's lag detector
 * sees to for a node that stays behind for good. A hold that lasts longer than {@link #HOLD_REPORT_AFTER} is reported
 * once at INFO, by each coordinator holding the node back: a fact worth having in the log next to a slow first apply,
 * not a problem of its own, which the lag detector reports.
 *
 * <p>The first state this node applies adds every other node, so each is asked once at startup; they answer at once.
 */
public final class ExternalWorkerReadiness implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ExternalWorkerReadiness.class);

    /** The pause before a failed await is sent again, while the node is still in the cluster state; doubles per failure. */
    static final TimeValue RETRY_DELAY = TimeValue.timeValueSeconds(1);
    /** The longest pause between two awaits of the same node. */
    static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueSeconds(10);
    /** How long a node may be held back before the fact is reported, once. */
    static final TimeValue HOLD_REPORT_AFTER = TimeValue.timeValueSeconds(30);

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    /**
     * The nodes still to answer, by ephemeral id: a node restarted under the same node id is a new node with its own first
     * apply. The value is the entry to compare against on completion, so an answer to an await of a superseded entry is a
     * no-op.
     */
    private final Map<String, Pending> pending = new ConcurrentHashMap<>();
    /**
     * The waits of {@link #awaitEligibleWorker} not yet decided, woken on every change to {@link #pending}: a release, a
     * removal, an addition. Invariant: {@link #pending} is mutated before the waiters are woken, and a waiter registers
     * before it first evaluates, so a change is never missed: either the wake-up finds the waiter registered, or the
     * waiter's own evaluation sees the change. A decided wait removes itself, so nothing of a query outlives its wait.
     */
    private final Set<Waiter> waiters = ConcurrentHashMap.newKeySet();
    /** {@link ExternalSourceSettings#WORKER_READY_TIMEOUT}, tracked live; bounds {@link #awaitEligibleWorker}. */
    private final AtomicReference<TimeValue> workerReadyTimeout = new AtomicReference<>();

    /** A node held back until the await of {@code version} on it answers. */
    private record Pending(DiscoveryNode node, long version) {}

    /**
     * @param enabled whether this node can ever dispatch external splits; when it cannot, nothing is asked of anyone and
     *                {@link #readyNodes} is the identity
     */
    public ExternalWorkerReadiness(ClusterService clusterService, Client client, ThreadPool threadPool, boolean enabled) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        // Seeded from the node settings when federation is unregistered (the key is unknown to ClusterSettings then) and
        // watched otherwise, as DataNodeComputeHandler does for its wait.
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(ExternalSourceSettings.WORKER_READY_TIMEOUT, workerReadyTimeout::set);
        if (enabled) {
            clusterService.addListener(this);
        }
    }

    /**
     * Completes {@code listener} once {@code satisfied} holds: at once if it does already, else on a later change to the
     * set of nodes held back, re-checked each time, and once more when the bound runs out, so a release that lands just
     * before the timer does not fail the query. Fails with the retryable {@link ExternalException.Condition#WORKER_NOT_READY}
     * after {@link ExternalSourceSettings#WORKER_READY_TIMEOUT} ({@code 0} fails at once, as on a worker), or at once
     * with the cancellation when {@code task} is cancelled. Exactly one outcome reaches {@code listener}; a throw out of
     * {@code satisfied} is that outcome.
     *
     * <p>{@code satisfied} is re-evaluated on the generic pool, never on the applier thread that may have woken the wait,
     * and must not run the query. {@code listener} is called with the caller's thread context restored, not the context of
     * the thread that woke it: a node answers under the system context its await was sent with, and a query that went on
     * in it would run as the system user. One DEBUG line when the wait starts and one when it ends, both naming
     * {@code what} the query is waiting to do.
     */
    void awaitEligibleWorker(BooleanSupplier satisfied, CancellableTask task, String what, ActionListener<Void> listener) {
        final ActionListener<Void> inCallerContext = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            threadPool.getThreadContext()
        );
        final boolean satisfiedNow;
        try {
            satisfiedNow = satisfied.getAsBoolean();
        } catch (Exception e) {
            inCallerContext.onFailure(e);
            return;
        }
        if (satisfiedNow) {
            inCallerContext.onResponse(null);
            return;
        }
        final TimeValue timeout = workerReadyTimeout.get();
        if (timeout.millis() <= 0) {
            inCallerContext.onFailure(timedOut(timeout));
            return;
        }
        logger.debug("waiting up to [{}] for an external worker to apply its cluster state before {}", timeout, what);
        new Waiter(satisfied, task, what, inCallerContext, timeout).start();
    }

    private static ExternalUnavailableException timedOut(TimeValue timeout) {
        return DataNodeComputeHandler.workerNotReady("waited [" + timeout + "] on the coordinator");
    }

    /**
     * One wait of {@link #awaitEligibleWorker}: registered in {@link #waiters}, with its timer and cancellation, ended by
     * the first outcome.
     */
    private final class Waiter {
        private final BooleanSupplier satisfied;
        private final CancellableTask task;
        private final String what;
        private final ActionListener<Void> listener;
        private final TimeValue timeout;
        private final long startNanos = System.nanoTime();
        private final AtomicBoolean done = new AtomicBoolean();
        private volatile Scheduler.Cancellable timer;

        Waiter(BooleanSupplier satisfied, CancellableTask task, String what, ActionListener<Void> listener, TimeValue timeout) {
            this.satisfied = satisfied;
            this.task = task;
            this.what = what;
            this.listener = listener;
            this.timeout = timeout;
        }

        void start() {
            try {
                timer = threadPool.schedule(() -> {
                    if (check() == false) {
                        complete(timedOut(timeout));
                    }
                }, timeout, threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                complete(e);
                return;
            }
            // Fires at once if already cancelled.
            task.addListener(() -> complete(new TaskCancelledException(task.getReasonCancelled())));
            // Registered before the first evaluation, which may see what a wake-up in between would have told it.
            waiters.add(this);
            check();
        }

        /** Woken by a change to the set; the evaluation goes to the generic pool, off the thread that changed it. */
        void wake() {
            if (done.get()) {
                return;
            }
            try {
                threadPool.generic().execute(this::check);
            } catch (EsRejectedExecutionException e) {
                complete(e);
            }
        }

        /** Evaluates the condition, completing the wait if it holds or if it throws; whether it completed. */
        private boolean check() {
            if (done.get()) {
                return true;
            }
            final boolean ok;
            try {
                ok = satisfied.getAsBoolean();
            } catch (Exception e) {
                complete(e);
                return true;
            }
            if (ok) {
                complete(null);
                return true;
            }
            return false;
        }

        private void complete(@Nullable Exception failure) {
            if (done.compareAndSet(false, true) == false) {
                return;
            }
            waiters.remove(this);
            final Scheduler.Cancellable scheduled = timer;
            if (scheduled != null) {
                scheduled.cancel();
            }
            logger.debug(
                () -> Strings.format(
                    "the wait for an external worker ended after [%s] before %s: %s",
                    TimeValue.timeValueNanos(System.nanoTime() - startNanos),
                    what,
                    failure == null ? "a worker is available" : failure.getMessage()
                )
            );
            if (failure == null) {
                listener.onResponse(null);
            } else {
                listener.onFailure(failure);
            }
        }
    }

    /** Wakes the waiters: {@link #pending} has changed. */
    private void notifyChanged() {
        for (Waiter waiter : waiters) {
            waiter.wake();
        }
    }

    /**
     * {@code nodes} without those still to confirm they applied the state that added them. The input itself when none is
     * pending, which is the steady state; a copy otherwise. The copy is for choosing workers from: when the held-back
     * node is the elected master, the copy names no master, since {@link DiscoveryNodes} does not allow a master id
     * without the node.
     */
    public DiscoveryNodes readyNodes(DiscoveryNodes nodes) {
        if (pending.isEmpty()) {
            return nodes;
        }
        DiscoveryNodes.Builder builder = null;
        for (Pending entry : pending.values()) {
            DiscoveryNode inState = nodes.get(entry.node().getId());
            if (inState != null && inState.getEphemeralId().equals(entry.node().getEphemeralId())) {
                if (builder == null) {
                    builder = DiscoveryNodes.builder(nodes);
                }
                builder.remove(inState);
                if (inState.getId().equals(nodes.getMasterNodeId())) {
                    builder.masterNodeId(null);
                }
            }
        }
        return builder == null ? nodes : builder.build();
    }

    /** The ephemeral ids of the nodes held back, for tests. */
    Set<String> pendingEphemeralIds() {
        return Set.copyOf(pending.keySet());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final DiscoveryNodes.Delta delta = event.nodesDelta();
        boolean removedAny = false;
        for (DiscoveryNode removed : delta.removedNodes()) {
            if (pending.remove(removed.getEphemeralId()) != null) {
                logger.debug("external worker [{}] left the cluster before confirming its cluster state", removed.getName());
                removedAny = true;
            }
        }
        if (removedAny) {
            notifyChanged();
        }
        if (delta.added() == false) {
            return;
        }
        final long version = event.state().version();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        for (DiscoveryNode added : delta.addedNodes()) {
            // This node plans from the state it applied, so it is never behind itself.
            if (added.getId().equals(localNodeId) || NodeEligibilityStrategy.isExternalWorker(added) == false) {
                continue;
            }
            final Pending entry = new Pending(added, version);
            pending.put(added.getEphemeralId(), entry);
            notifyChanged();
            logger.debug("holding external worker [{}] back until it has applied cluster state version [{}]", added.getName(), version);
            // Off the applier thread: the request goes through the action filters and is serialised, none of which the
            // applier should carry, least of all once per node when this node applies its first state.
            run(entry, TimeValue.ZERO, () -> await(entry, 0));
            run(entry, HOLD_REPORT_AFTER, () -> reportStillHeld(entry));
        }
    }

    /** Runs {@code task} on the generic pool after {@code delay}, if {@code entry} is still the one held back then. */
    private void run(Pending entry, TimeValue delay, Runnable task) {
        final Runnable ifCurrent = () -> {
            if (isCurrent(entry)) {
                task.run();
            }
        };
        try {
            if (delay.equals(TimeValue.ZERO)) {
                threadPool.generic().execute(ifCurrent);
            } else {
                threadPool.schedule(ifCurrent, delay, threadPool.generic());
            }
        } catch (EsRejectedExecutionException e) {
            // Only on shutdown: the generic pool queues without bound, and so does the scheduler. Nothing to ask on
            // behalf of any more; the entry stays, and so does the node's exclusion, which no longer matters.
            logger.debug(() -> Strings.format("not asking external worker [%s]", entry.node().getName()), e);
        }
    }

    /** Sends the await; {@code attempt} counts the failures so far, for the backoff of the next one. */
    private void await(Pending entry, int attempt) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            // An internal action sent on the node's own behalf, whatever context the caller's thread carries.
            threadContext.markAsSystemContext();
            client.execute(
                TransportAwaitClusterStateVersionAppliedAction.TYPE,
                new AwaitClusterStateVersionAppliedRequest(entry.version(), TimeValue.MINUS_ONE, entry.node()),
                ActionListener.wrap(response -> {
                    if (response.hasFailures()) {
                        onFailure(entry, attempt, response.failures().getFirst());
                    } else {
                        release(entry, "it has applied cluster state version [" + entry.version() + "]");
                    }
                }, e -> onFailure(entry, attempt, e))
            );
        } catch (Exception e) {
            onFailure(entry, attempt, e);
        }
    }

    private boolean isCurrent(Pending entry) {
        return pending.get(entry.node().getEphemeralId()) == entry;
    }

    private void release(Pending entry, String why) {
        if (pending.remove(entry.node().getEphemeralId(), entry)) {
            logger.debug("external worker [{}] is eligible for external splits: {}", entry.node().getName(), why);
            notifyChanged();
        }
    }

    private void onFailure(Pending entry, int attempt, Exception failure) {
        if (isCurrent(entry) == false) {
            return;
        }
        if (ExceptionsHelper.unwrap(failure, ActionNotFoundTransportException.class) != null) {
            // A node older than the action cannot be asked. It was never held back before either; what protects a query
            // sent to it while it catches up is its own wait, if it has one.
            release(entry, "it cannot be asked for its cluster state version");
            return;
        }
        final TimeValue delay = retryDelay(attempt);
        logger.debug(
            () -> Strings.format(
                "could not confirm cluster state version [%s] on external worker [%s]; asking again in [%s]",
                entry.version(),
                entry.node().getName(),
                delay
            ),
            failure
        );
        run(entry, delay, () -> await(entry, attempt + 1));
    }

    /** {@link #RETRY_DELAY} doubled per failure so far, capped at {@link #MAX_RETRY_DELAY}. */
    static TimeValue retryDelay(int failures) {
        final long millis = RETRY_DELAY.millis() << Math.min(failures, 10);
        return TimeValue.timeValueMillis(Math.min(millis, MAX_RETRY_DELAY.millis()));
    }

    private void reportStillHeld(Pending entry) {
        logger.info(
            "external worker [{}] has not confirmed cluster state version [{}] after [{}]; it is given no external splits until it does",
            entry.node().getName(),
            entry.version(),
            HOLD_REPORT_AFTER
        );
    }
}
