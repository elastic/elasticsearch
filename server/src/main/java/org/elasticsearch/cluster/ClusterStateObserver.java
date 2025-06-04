/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A utility class which simplifies interacting with the cluster state in cases where
 * one tries to take action based on the current state but may want to wait for a new state
 * and retry upon failure.
 */
public class ClusterStateObserver {

    protected final Logger logger;

    public static final Predicate<ClusterState> NON_NULL_MASTER_PREDICATE = state -> state.nodes().getMasterNode() != null;

    private final ClusterApplierService clusterApplierService;
    private final ThreadPool threadPool;
    private final ThreadContext contextHolder;
    volatile TimeValue timeOutValue;

    private volatile long lastObservedVersion;
    final TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();
    // observingContext is not null when waiting on cluster state changes
    final AtomicReference<ObservingContext> observingContext = new AtomicReference<>(null);
    volatile Long startTimeMS;
    volatile boolean timedOut;

    /**
     * @param timeout        a global timeout for this observer. After it has expired the observer
     *                       will fail any existing or new #waitForNextChange calls. Set to null
     *                       to wait indefinitely
     */
    public ClusterStateObserver(ClusterService clusterService, @Nullable TimeValue timeout, Logger logger, ThreadContext contextHolder) {
        this(clusterService.state(), clusterService, timeout, logger, contextHolder);
    }

    /**
     * @param timeout        a global timeout for this observer. After it has expired the observer
     *                       will fail any existing or new #waitForNextChange calls. Set to null
     *                       to wait indefinitely
     */
    public ClusterStateObserver(
        ClusterState initialState,
        ClusterService clusterService,
        @Nullable TimeValue timeout,
        Logger logger,
        ThreadContext contextHolder
    ) {
        this(initialState.version(), clusterService.getClusterApplierService(), timeout, logger, contextHolder);
    }

    public ClusterStateObserver(
        long initialVersion,
        ClusterApplierService clusterApplierService,
        @Nullable TimeValue timeout,
        Logger logger,
        ThreadContext contextHolder
    ) {
        this.clusterApplierService = clusterApplierService;
        this.threadPool = clusterApplierService.threadPool();
        this.lastObservedVersion = initialVersion;
        this.timeOutValue = timeout;
        if (timeOutValue != null) {
            this.startTimeMS = threadPool.relativeTimeInMillis();
        }
        this.logger = logger;
        this.contextHolder = contextHolder;
    }

    /** sets the last observed state to the currently applied cluster state and returns it */
    public ClusterState setAndGetObservedState() {
        if (observingContext.get() != null) {
            throw new ElasticsearchException("cannot set current cluster state while waiting for a cluster state change");
        }
        ClusterState clusterState = clusterApplierService.state();
        lastObservedVersion = clusterState.version();
        return clusterState;
    }

    /** indicates whether this observer has timed out */
    public boolean isTimedOut() {
        return timedOut;
    }

    public void waitForNextChange(Listener listener) {
        waitForNextChange(listener, Predicates.always());
    }

    public void waitForNextChange(Listener listener, @Nullable TimeValue timeOutValue) {
        waitForNextChange(listener, Predicates.always(), timeOutValue);
    }

    public void waitForNextChange(Listener listener, Predicate<ClusterState> statePredicate) {
        waitForNextChange(listener, statePredicate, null);
    }

    /**
     * Wait for the next cluster state which satisfies statePredicate
     *
     * @param listener        callback listener
     * @param statePredicate predicate to check whether cluster state changes are relevant and the callback should be called
     * @param timeOutValue    a timeout for waiting. If null the global observer timeout will be used.
     */
    public void waitForNextChange(Listener listener, Predicate<ClusterState> statePredicate, @Nullable TimeValue timeOutValue) {
        listener = new ContextPreservingListener(listener, contextHolder.newRestorableContext(false));
        if (observingContext.get() != null) {
            throw new ElasticsearchException("already waiting for a cluster state change");
        }

        Long timeoutTimeLeftMS;
        if (timeOutValue == null) {
            timeOutValue = this.timeOutValue;
            if (timeOutValue != null) {
                long timeSinceStartMS = threadPool.relativeTimeInMillis() - startTimeMS;
                timeoutTimeLeftMS = timeOutValue.millis() - timeSinceStartMS;
                if (timeoutTimeLeftMS <= 0L) {
                    // things have timeout while we were busy -> notify
                    logger.trace(
                        "observer timed out. notifying listener. timeout setting [{}], time since start [{}]",
                        timeOutValue,
                        new TimeValue(timeSinceStartMS)
                    );
                    // update to latest, in case people want to retry
                    timedOut = true;
                    lastObservedVersion = clusterApplierService.state().version();
                    listener.onTimeout(timeOutValue);
                    return;
                }
            } else {
                timeoutTimeLeftMS = null;
            }
        } else {
            this.startTimeMS = threadPool.relativeTimeInMillis();
            this.timeOutValue = timeOutValue;
            timeoutTimeLeftMS = timeOutValue.millis();
            timedOut = false;
        }

        // sample a new state. This state maybe *older* than the supplied state if we are called from an applier,
        // which wants to wait for something else to happen
        ClusterState newState = clusterApplierService.state();
        if (lastObservedVersion < newState.version() && statePredicate.test(newState)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate ({})", newState);
            lastObservedVersion = newState.version();
            listener.onNewClusterState(newState);
        } else {
            logger.trace("observer: sampled state rejected by predicate ({}). adding listener to ClusterService", newState);
            final ObservingContext context = new ObservingContext(listener, statePredicate);
            if (observingContext.compareAndSet(null, context) == false) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            clusterApplierService.addTimeoutListener(
                timeoutTimeLeftMS == null ? null : new TimeValue(timeoutTimeLeftMS),
                clusterStateListener
            );
        }
    }

    /**
     * Waits for the cluster state to match a given predicate. Unlike {@link #waitForNextChange} this method checks whether the current
     * state matches the predicate first and resolves the listener directly if it matches without waiting for another cluster state update.
     *
     * @param clusterService cluster service
     * @param threadContext  thread context to resolve listener in
     * @param listener       listener to resolve once state matches the predicate
     * @param statePredicate predicate the cluster state has to match
     * @param timeout        timeout for the wait or {@code null} for no timeout
     * @param logger         logger to use for logging observer messages
     */
    public static void waitForState(
        ClusterService clusterService,
        ThreadContext threadContext,
        Listener listener,
        Predicate<ClusterState> statePredicate,
        @Nullable TimeValue timeout,
        Logger logger
    ) {
        final ClusterState initialState = clusterService.state();
        if (statePredicate.test(initialState)) {
            // short-cut in case the state matches the predicate already
            listener.onNewClusterState(initialState);
            return;
        }
        ClusterStateObserver observer = new ClusterStateObserver(initialState, clusterService, timeout, logger, threadContext);
        observer.waitForNextChange(listener, statePredicate);
    }

    class ObserverClusterStateListener implements TimeoutClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            final ClusterState state = event.state();
            if (context.statePredicate.test(state)) {
                if (observingContext.compareAndSet(context, null)) {
                    clusterApplierService.removeTimeoutListener(this);
                    logger.trace("observer: accepting cluster state change ({})", state);
                    lastObservedVersion = state.version();
                    try {
                        context.listener.onNewClusterState(state);
                    } catch (Exception e) {
                        logUnexpectedException(e, "cluster state version [%d]", state.version());
                    }
                } else {
                    logger.trace(
                        "observer: predicate approved change but observing context has changed "
                            + "- ignoring (new cluster state version [{}])",
                        state.version()
                    );
                }
            } else {
                logger.trace("observer: predicate rejected change (new cluster state version [{}])", state.version());
            }
        }

        @Override
        public void postAdded() {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            ClusterState newState = clusterApplierService.state();
            if (lastObservedVersion < newState.version() && context.statePredicate.test(newState)) {
                // double check we're still listening
                if (observingContext.compareAndSet(context, null)) {
                    logger.trace("observer: post adding listener: accepting current cluster state ({})", newState);
                    clusterApplierService.removeTimeoutListener(this);
                    lastObservedVersion = newState.version();
                    try {
                        context.listener.onNewClusterState(newState);
                    } catch (Exception e) {
                        logUnexpectedException(e, "cluster state version [%d]", newState.version());
                    }
                } else {
                    logger.trace(
                        "observer: postAdded - predicate approved state but observing context has changed - ignoring ({})",
                        newState
                    );
                }
            } else {
                logger.trace("observer: postAdded - predicate rejected state ({})", newState);
            }
        }

        @Override
        public void onClose() {
            ObservingContext context = observingContext.getAndSet(null);

            if (context != null) {
                logger.trace("observer: cluster service closed. notifying listener.");
                clusterApplierService.removeTimeoutListener(this);
                try {
                    context.listener.onClusterServiceClose();
                } catch (Exception e) {
                    logUnexpectedException(e, "cluster service close");
                }
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            ObservingContext context = observingContext.getAndSet(null);
            if (context != null) {
                clusterApplierService.removeTimeoutListener(this);
                long timeSinceStartMS = threadPool.relativeTimeInMillis() - startTimeMS;
                logger.trace(
                    "observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]",
                    timeOutValue,
                    new TimeValue(timeSinceStartMS)
                );
                // update to latest, in case people want to retry
                lastObservedVersion = clusterApplierService.state().version();
                timedOut = true;
                try {
                    context.listener.onTimeout(timeOutValue);
                } catch (Exception e) {
                    logUnexpectedException(e, "timeout after [%s]", timeOutValue);
                }
            }
        }

        @Override
        public String toString() {
            return "ClusterStateObserver[" + observingContext.get() + "]";
        }

        private void logUnexpectedException(Exception exception, String format, Object... args) {
            final var illegalStateException = new IllegalStateException(
                Strings.format(
                    "unexpected exception processing %s in context [%s]",
                    Strings.format(format, args),
                    ObserverClusterStateListener.this
                ),
                exception
            );
            logger.error(illegalStateException.getMessage(), illegalStateException);
            assert false : illegalStateException;
        }
    }

    public interface Listener {

        /**
         * Called when a new state is observed. Implementations should avoid doing heavy operations on the calling thread and fork to
         * a threadpool if necessary to avoid blocking the {@link ClusterApplierService}. Note that operations such as sending a new
         * request (e.g. via {@link org.elasticsearch.client.internal.Client} or {@link org.elasticsearch.transport.TransportService})
         * is cheap enough to be performed without forking.
         */
        void onNewClusterState(ClusterState state);

        /** called when the cluster service is closed */
        void onClusterServiceClose();

        /**
         * Called when the {@link ClusterStateObserver} times out while waiting for a new matching cluster state if a timeout is
         * used when creating the observer. Upon timeout, {@code onTimeout} is called on the GENERIC threadpool.
         */
        void onTimeout(TimeValue timeout);
    }

    static class ObservingContext {
        public final Listener listener;
        public final Predicate<ClusterState> statePredicate;

        ObservingContext(Listener listener, Predicate<ClusterState> statePredicate) {
            this.listener = listener;
            this.statePredicate = statePredicate;
        }

        @Override
        public String toString() {
            return "ObservingContext[" + listener + "]";
        }
    }

    private static final class ContextPreservingListener implements Listener {
        private final Listener delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;

        private ContextPreservingListener(Listener delegate, Supplier<ThreadContext.StoredContext> contextSupplier) {
            this.contextSupplier = contextSupplier;
            this.delegate = delegate;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try (ThreadContext.StoredContext context = contextSupplier.get()) {
                delegate.onNewClusterState(state);
            }
        }

        @Override
        public void onClusterServiceClose() {
            try (ThreadContext.StoredContext context = contextSupplier.get()) {
                delegate.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            try (ThreadContext.StoredContext context = contextSupplier.get()) {
                delegate.onTimeout(timeout);
            }
        }

        @Override
        public String toString() {
            return "ContextPreservingListener[" + delegate + "]";
        }
    }
}
