/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Keeps a node that has just joined the cluster out of this coordinator's pool of external workers until the node has
 * applied the cluster state that added it.
 *
 * <p>A node accepts transport requests before it has applied any cluster state, and a coordinator sees it as an external
 * worker as soon as it applies the join; until the node's own first apply completes, every external split sent to it
 * fails or waits. A shard has no such window, being routable only once its node reported it started, but external
 * splits are routed by role. So each added worker is asked, through
 * {@link TransportAwaitClusterStateVersionAppliedAction}, to confirm it has applied the version that added it, and is
 * held back until it answers. The hold is installed by the applier, before the state that adds the node is visible, so a
 * query cannot see the node without seeing it held back; the node is forgotten by the listener, once the state without
 * it is visible. A node older than the action ({@link #AWAIT_ACTION_SINCE}) is neither asked nor held back.
 *
 * <p>When every worker there is has just joined, which is only possible on a coordinator that is not itself a worker, the
 * query waits here for one of them ({@link #awaitEligibleWorker}), bounded by {@code esql.external.worker_ready_timeout}.
 * A node that is behind a later state, a dataset change, say, is not held back; the worker-side wait in
 * {@code DataNodeComputeHandler} covers that.
 */
public final class ExternalWorkerReadiness implements ClusterStateApplier, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ExternalWorkerReadiness.class);

    /** The pause before a failed await is sent again; doubles per failure, up to {@link #MAX_RETRY_DELAY}. */
    static final TimeValue RETRY_DELAY = TimeValue.timeValueSeconds(1);
    static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueSeconds(10);
    /** How long a node may be held back before the fact is reported, once, at INFO. */
    static final TimeValue HOLD_REPORT_AFTER = TimeValue.timeValueSeconds(30);
    /**
     * The first version with {@link TransportAwaitClusterStateVersionAppliedAction}. An older node has no handler for it,
     * which trips an assertion on a node that has them enabled, so it is not asked; nor is it held back, as it never was
     * before the action existed.
     */
    static final Version AWAIT_ACTION_SINCE = Version.V_9_4_0;

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    /** The nodes still to answer, by ephemeral id, so that a node restarted under the same node id is a new node. */
    private final Map<String, Pending> pending = new ConcurrentHashMap<>();
    /**
     * The waits of {@link #awaitEligibleWorker} not yet decided, woken on every change to what {@link #readyNodes} can
     * return: a release, and the visible state gaining or losing a node. The change is made before the waiters are woken
     * and a waiter re-evaluates after registering, so no change is missed.
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
        // Seeded from the node settings when federation is unregistered (the key is unknown to ClusterSettings then).
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(ExternalSourceSettings.WORKER_READY_TIMEOUT, workerReadyTimeout::set);
        if (enabled) {
            clusterService.addStateApplier(this);
            clusterService.addListener(this);
        }
    }

    /**
     * Completes {@code listener} once {@code satisfied} holds: at once if it does already, else on a later change to the
     * ready nodes, and once more when the bound runs out, so a release that lands just before the timer does not fail the
     * query. Fails with the retryable {@link ExternalException.Condition#WORKER_NOT_READY} after
     * {@link ExternalSourceSettings#WORKER_READY_TIMEOUT} ({@code 0} fails at once), or with the cancellation when
     * {@code task} is cancelled; a throw out of {@code satisfied} is the outcome too. {@code satisfied} is re-evaluated on
     * the generic pool, never on the applier thread, and {@code listener} is called in the caller's thread context, not in
     * the system context a node's answer arrives under.
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

    /** One wait of {@link #awaitEligibleWorker}: registered in {@link #waiters} from {@link #start} until its outcome. */
    private final class Waiter {
        private final BooleanSupplier satisfied;
        private final CancellableTask task;
        private final String what;
        private final ActionListener<Void> listener;
        private final TimeValue timeout;
        private final long startNanos = System.nanoTime();
        private final SubscribableListener<Void> outcome = new SubscribableListener<>();
        private volatile Scheduler.Cancellable timer;

        Waiter(BooleanSupplier satisfied, CancellableTask task, String what, ActionListener<Void> listener, TimeValue timeout) {
            this.satisfied = satisfied;
            this.task = task;
            this.what = what;
            this.listener = listener;
            this.timeout = timeout;
        }

        /**
         * Arms the bound and the cancellation, registers, then subscribes the outcome, which unregisters: an outcome
         * reached before the subscription, a task already cancelled, say, is delivered by it inline.
         */
        void start() {
            try {
                timer = threadPool.schedule(() -> {
                    if (tryComplete() == false) {
                        outcome.onFailure(timedOut(timeout));
                    }
                }, timeout, threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                outcome.onFailure(e);
            }
            task.addListener(() -> outcome.onFailure(new TaskCancelledException(task.getReasonCancelled())));
            waiters.add(this);
            outcome.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    ended(null);
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    ended(e);
                    listener.onFailure(e);
                }
            });
            tryComplete();
        }

        /** Woken by a change to the ready nodes; the evaluation goes to the generic pool, off the thread that changed them. */
        void wake() {
            if (outcome.isDone()) {
                return;
            }
            try {
                threadPool.generic().execute(this::tryComplete);
            } catch (EsRejectedExecutionException e) {
                outcome.onFailure(e);
            }
        }

        /** Completes the wait if the condition holds or throws; whether it is complete. */
        private boolean tryComplete() {
            if (outcome.isDone()) {
                return true;
            }
            final boolean ok;
            try {
                ok = satisfied.getAsBoolean();
            } catch (Exception e) {
                outcome.onFailure(e);
                return true;
            }
            if (ok) {
                outcome.onResponse(null);
                return true;
            }
            return false;
        }

        private void ended(@Nullable Exception failure) {
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
        }
    }

    private void notifyChanged() {
        for (Waiter waiter : waiters) {
            waiter.wake();
        }
    }

    /**
     * {@code nodes} without those still to confirm they applied the state that added them: the input itself when none is
     * pending, a copy otherwise. The copy names no master when the held-back node is the elected master, since
     * {@link DiscoveryNodes} does not allow a master id without the node.
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

    /** The waits of {@link #awaitEligibleWorker} not yet decided, for tests. */
    int waiterCount() {
        return waiters.size();
    }

    /**
     * Holds every added worker back, before the state that adds it is visible. The waits are not woken here: a wait
     * evaluates against the visible state, so it is woken by {@link #clusterChanged}, once the state is.
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final DiscoveryNodes.Delta delta = event.nodesDelta();
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
            if (canBeAsked(added) == false) {
                logger.debug(
                    "not holding external worker [{}] back: at version [{}] it cannot be asked for its cluster state version",
                    added.getName(),
                    added.getVersionInformation().nodeVersion()
                );
                continue;
            }
            final Pending entry = new Pending(added, version);
            pending.put(added.getEphemeralId(), entry);
            logger.debug("holding external worker [{}] back until it has applied cluster state version [{}]", added.getName(), version);
            // Off the applier thread: the request goes through the action filters and is serialised.
            run(entry, TimeValue.ZERO, () -> await(entry, 0));
            run(entry, HOLD_REPORT_AFTER, () -> reportStillHeld(entry));
        }
    }

    /**
     * Forgets every removed worker, and wakes the waits on any change to the nodes, once the state is visible: a wait woken
     * here sees the node that joined, held back or not, and does not see the node that left.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final DiscoveryNodes.Delta delta = event.nodesDelta();
        for (DiscoveryNode removed : delta.removedNodes()) {
            if (pending.remove(removed.getEphemeralId()) != null) {
                logger.debug("external worker [{}] left the cluster before confirming its cluster state", removed.getName());
            }
        }
        if (delta.added() || delta.removed()) {
            notifyChanged();
        }
    }

    /** Whether {@code node} has the action to ask: at {@link #AWAIT_ACTION_SINCE} or later. */
    static boolean canBeAsked(DiscoveryNode node) {
        return node.getVersionInformation().nodeVersion().onOrAfter(AWAIT_ACTION_SINCE);
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
            // Only on shutdown: the entry stays, and so does the node's exclusion, which no longer matters.
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
            // Not expected, given AWAIT_ACTION_SINCE; released rather than asked again for good.
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
