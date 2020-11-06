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

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
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

    private final Predicate<ClusterState> MATCH_ALL_CHANGES_PREDICATE = state -> true;

    private final ClusterApplierService clusterApplierService;
    private final ThreadPool threadPool;
    private final ThreadContext contextHolder;
    volatile TimeValue timeOutValue;


    final AtomicReference<StoredState> lastObservedState;
    final TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();
    // observingContext is not null when waiting on cluster state changes
    final AtomicReference<ObservingContext> observingContext = new AtomicReference<>(null);
    volatile Long startTimeMS;
    volatile boolean timedOut;


    public ClusterStateObserver(ClusterService clusterService, Logger logger, ThreadContext contextHolder) {
        this(clusterService, new TimeValue(60000), logger, contextHolder);
    }

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
    public ClusterStateObserver(ClusterState initialState, ClusterService clusterService, @Nullable TimeValue timeout, Logger logger,
                                ThreadContext contextHolder) {
        this(initialState, clusterService.getClusterApplierService(), timeout, logger, contextHolder);
    }

    public ClusterStateObserver(ClusterState initialState, ClusterApplierService clusterApplierService, @Nullable TimeValue timeout,
                                Logger logger, ThreadContext contextHolder) {
        this.clusterApplierService = clusterApplierService;
        this.threadPool = clusterApplierService.threadPool();
        this.lastObservedState = new AtomicReference<>(new StoredState(initialState));
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
        lastObservedState.set(new StoredState(clusterState));
        return clusterState;
    }

    /** indicates whether this observer has timed out */
    public boolean isTimedOut() {
        return timedOut;
    }

    public void waitForNextChange(Listener listener) {
        waitForNextChange(listener, MATCH_ALL_CHANGES_PREDICATE);
    }

    public void waitForNextChange(Listener listener, @Nullable TimeValue timeOutValue) {
        waitForNextChange(listener, MATCH_ALL_CHANGES_PREDICATE, timeOutValue);
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
                    logger.trace("observer timed out. notifying listener. timeout setting [{}], time since start [{}]",
                        timeOutValue, new TimeValue(timeSinceStartMS));
                    // update to latest, in case people want to retry
                    timedOut = true;
                    lastObservedState.set(new StoredState(clusterApplierService.state()));
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
        if (lastObservedState.get().isOlderOrDifferentMaster(newState) && statePredicate.test(newState)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate ({})", newState);
            lastObservedState.set(new StoredState(newState));
            listener.onNewClusterState(newState);
        } else {
            logger.trace("observer: sampled state rejected by predicate ({}). adding listener to ClusterService", newState);
            final ObservingContext context = new ObservingContext(listener, statePredicate);
            if (!observingContext.compareAndSet(null, context)) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            clusterApplierService.addTimeoutListener(timeoutTimeLeftMS == null ?
                null : new TimeValue(timeoutTimeLeftMS), clusterStateListener);
        }
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
                    lastObservedState.set(new StoredState(state));
                    context.listener.onNewClusterState(state);
                } else {
                    logger.trace("observer: predicate approved change but observing context has changed " +
                        "- ignoring (new cluster state version [{}])", state.version());
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
            if (lastObservedState.get().isOlderOrDifferentMaster(newState) && context.statePredicate.test(newState)) {
                // double check we're still listening
                if (observingContext.compareAndSet(context, null)) {
                    logger.trace("observer: post adding listener: accepting current cluster state ({})", newState);
                    clusterApplierService.removeTimeoutListener(this);
                    lastObservedState.set(new StoredState(newState));
                    context.listener.onNewClusterState(newState);
                } else {
                    logger.trace("observer: postAdded - predicate approved state but observing context has changed - ignoring ({})",
                        newState);
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
                context.listener.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            ObservingContext context = observingContext.getAndSet(null);
            if (context != null) {
                clusterApplierService.removeTimeoutListener(this);
                long timeSinceStartMS = threadPool.relativeTimeInMillis() - startTimeMS;
                logger.trace("observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]",
                    timeOutValue, new TimeValue(timeSinceStartMS));
                // update to latest, in case people want to retry
                lastObservedState.set(new StoredState(clusterApplierService.state()));
                timedOut = true;
                context.listener.onTimeout(timeOutValue);
            }
        }

        @Override
        public String toString() {
            return "ClusterStateObserver[" + observingContext.get() + "]";
        }
    }

    /**
     * The observer considers two cluster states to be the same if they have the same version and master node id (i.e. null or set)
     */
    private static class StoredState {
        private final String masterNodeId;
        private final long version;

        StoredState(ClusterState clusterState) {
            this.masterNodeId = clusterState.nodes().getMasterNodeId();
            this.version = clusterState.version();
        }

        /**
         * returns true if stored state is older then given state or they are from a different master, meaning they can't be compared
         * */
        public boolean isOlderOrDifferentMaster(ClusterState clusterState) {
            return version < clusterState.version() || Objects.equals(masterNodeId, clusterState.nodes().getMasterNodeId()) == false;
        }
    }

    public interface Listener {

        /** called when a new state is observed */
        void onNewClusterState(ClusterState state);

        /** called when the cluster service is closed */
        void onClusterServiceClose();

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
            try (ThreadContext.StoredContext context  = contextSupplier.get()) {
                delegate.onNewClusterState(state);
            }
        }

        @Override
        public void onClusterServiceClose() {
            try (ThreadContext.StoredContext context  = contextSupplier.get()) {
                delegate.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            try (ThreadContext.StoredContext context  = contextSupplier.get()) {
                delegate.onTimeout(timeout);
            }
        }

        @Override
        public String toString() {
            return "ContextPreservingListener[" + delegate + "]";
        }
    }
}
