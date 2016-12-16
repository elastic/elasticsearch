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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * A utility class which simplifies interacting with the cluster state in cases where
 * one tries to take action based on the current state but may want to wait for a new state
 * and retry upon failure.
 */
public class ClusterStateObserver {

    protected final Logger logger;

    private final Predicate<ClusterState> MATCH_ALL_CHANGES_PREDICATE = state -> true;

    private final ClusterService clusterService;
    private final ThreadContext contextHolder;
    volatile TimeValue timeOutValue;


    final AtomicReference<ClusterState> lastObservedState;
    final TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();
    // observingContext is not null when waiting on cluster state changes
    final AtomicReference<ObservingContext> observingContext = new AtomicReference<>(null);
    volatile Long startTimeNS;
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
        this.clusterService = clusterService;
        this.lastObservedState = new AtomicReference<>(initialState);
        this.timeOutValue = timeout;
        if (timeOutValue != null) {
            this.startTimeNS = System.nanoTime();
        }
        this.logger = logger;
        this.contextHolder = contextHolder;
    }

    /** last cluster state and status observed by this observer. Note that this may not be the current one */
    public ClusterState observedState() {
        ClusterState state = lastObservedState.get();
        assert state != null;
        return state;
    }

    /** indicates whether this observer has timedout */
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

        if (observingContext.get() != null) {
            throw new ElasticsearchException("already waiting for a cluster state change");
        }

        Long timeoutTimeLeftMS;
        if (timeOutValue == null) {
            timeOutValue = this.timeOutValue;
            if (timeOutValue != null) {
                long timeSinceStartMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeNS);
                timeoutTimeLeftMS = timeOutValue.millis() - timeSinceStartMS;
                if (timeoutTimeLeftMS <= 0L) {
                    // things have timeout while we were busy -> notify
                    logger.trace("observer timed out. notifying listener. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStartMS));
                    // update to latest, in case people want to retry
                    timedOut = true;
                    lastObservedState.set(clusterService.state());
                    listener.onTimeout(timeOutValue);
                    return;
                }
            } else {
                timeoutTimeLeftMS = null;
            }
        } else {
            this.startTimeNS = System.nanoTime();
            this.timeOutValue = timeOutValue;
            timeoutTimeLeftMS = timeOutValue.millis();
            timedOut = false;
        }

        // sample a new state
        ClusterState newState = clusterService.state();
        ClusterState lastState = lastObservedState.get();
        if (newState != lastState && statePredicate.test(newState)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate ({})", newState);
            lastObservedState.set(newState);
            listener.onNewClusterState(newState);
        } else {
            logger.trace("observer: sampled state rejected by predicate ({}). adding listener to ClusterService", newState);
            ObservingContext context =
                new ObservingContext(new ContextPreservingListener(listener, contextHolder.newStoredContext()), statePredicate);
            if (!observingContext.compareAndSet(null, context)) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            clusterService.addTimeoutListener(timeoutTimeLeftMS == null ? null : new TimeValue(timeoutTimeLeftMS), clusterStateListener);
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
                    clusterService.removeTimeoutListener(this);
                    logger.trace("observer: accepting cluster state change ({})", state);
                    lastObservedState.set(state);
                    context.listener.onNewClusterState(state);
                } else {
                    logger.trace("observer: predicate approved change but observing context has changed - ignoring (new cluster state version [{}])", state.version());
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
            ClusterState newState = clusterService.state();
            ClusterState lastState = lastObservedState.get();
            if (newState != lastState && context.statePredicate.test(newState)) {
                // double check we're still listening
                if (observingContext.compareAndSet(context, null)) {
                    logger.trace("observer: post adding listener: accepting current cluster state ({})", newState);
                    clusterService.removeTimeoutListener(this);
                    lastObservedState.set(newState);
                    context.listener.onNewClusterState(newState);
                } else {
                    logger.trace("observer: postAdded - predicate approved state but observing context has changed - ignoring ({})", newState);
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
                clusterService.removeTimeoutListener(this);
                context.listener.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            ObservingContext context = observingContext.getAndSet(null);
            if (context != null) {
                clusterService.removeTimeoutListener(this);
                long timeSinceStartMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeNS);
                logger.trace("observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStartMS));
                // update to latest, in case people want to retry
                lastObservedState.set(clusterService.state());
                timedOut = true;
                context.listener.onTimeout(timeOutValue);
            }
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

        public ObservingContext(Listener listener, Predicate<ClusterState> statePredicate) {
            this.listener = listener;
            this.statePredicate = statePredicate;
        }
    }

    private static final class ContextPreservingListener implements Listener {
        private final Listener delegate;
        private final ThreadContext.StoredContext tempContext;


        private ContextPreservingListener(Listener delegate, ThreadContext.StoredContext storedContext) {
            this.tempContext = storedContext;
            this.delegate = delegate;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            tempContext.restore();
            delegate.onNewClusterState(state);
        }

        @Override
        public void onClusterServiceClose() {
            tempContext.restore();
            delegate.onClusterServiceClose();
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            tempContext.restore();
            delegate.onTimeout(timeout);
        }
    }
}
