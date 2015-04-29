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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A utility class which simplifies interacting with the cluster state in cases where
 * one tries to take action based on the current state but may want to wait for a new state
 * and retry upon failure.
 */
public class ClusterStateObserver {

    protected final ESLogger logger;

    public final ChangePredicate MATCH_ALL_CHANGES_PREDICATE = new EventPredicate() {

        @Override
        public boolean apply(ClusterChangedEvent changedEvent) {
            return changedEvent.previousState().version() != changedEvent.state().version();
        }
    };

    private final  ClusterService clusterService;
    volatile TimeValue timeOutValue;


    final AtomicReference<ObservedState> lastObservedState;
    final TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();
    // observingContext is not null when waiting on cluster state changes
    final AtomicReference<ObservingContext> observingContext = new AtomicReference<ObservingContext>(null);
    volatile Long startTime;
    volatile boolean timedOut;


    public ClusterStateObserver(ClusterService clusterService, ESLogger logger) {
        this(clusterService, new TimeValue(60000), logger);
    }

    /**
     * @param clusterService
     * @param timeout        a global timeout for this observer. After it has expired the observer
     *                       will fail any existing or new #waitForNextChange calls.
     */
    public ClusterStateObserver(ClusterService clusterService, TimeValue timeout, ESLogger logger) {
        this.clusterService = clusterService;
        this.lastObservedState = new AtomicReference<>(new ObservedState(clusterService.state()));
        this.timeOutValue = timeout;
        if (timeOutValue != null) {
            this.startTime = System.currentTimeMillis();
        }
        this.logger = logger;
    }

    /** last cluster state observer by this observer. Note that this may not be the current one */
    public ClusterState observedState() {
        ObservedState state = lastObservedState.get();
        assert state != null;
        return state.clusterState;
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

    public void waitForNextChange(Listener listener, ChangePredicate changePredicate) {
        waitForNextChange(listener, changePredicate, null);
    }

    /**
     * Wait for the next cluster state which satisfies changePredicate
     *
     * @param listener        callback listener
     * @param changePredicate predicate to check whether cluster state changes are relevant and the callback should be called
     * @param timeOutValue    a timeout for waiting. If null the global observer timeout will be used.
     */
    public void waitForNextChange(Listener listener, ChangePredicate changePredicate, @Nullable TimeValue timeOutValue) {

        if (observingContext.get() != null) {
            throw new ElasticsearchException("already waiting for a cluster state change");
        }

        Long timeoutTimeLeft;
        if (timeOutValue == null) {
            timeOutValue = this.timeOutValue;
            if (timeOutValue != null) {
                long timeSinceStart = System.currentTimeMillis() - startTime;
                timeoutTimeLeft = timeOutValue.millis() - timeSinceStart;
                if (timeoutTimeLeft <= 0l) {
                    // things have timeout while we were busy -> notify
                    logger.debug("observer timed out. notifying listener. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStart));
                    // update to latest, in case people want to retry
                    timedOut = true;
                    lastObservedState.set(new ObservedState(clusterService.state()));
                    listener.onTimeout(timeOutValue);
                    return;
                }
            } else {
                timeoutTimeLeft = null;
            }
        } else {
            this.startTime = System.currentTimeMillis();
            this.timeOutValue = timeOutValue;
            timeoutTimeLeft = timeOutValue.millis();
            timedOut = false;
        }

        // sample a new state
        ObservedState newState = new ObservedState(clusterService.state());
        ObservedState lastState = lastObservedState.get();
        if (changePredicate.apply(lastState.clusterState, lastState.status, newState.clusterState, newState.status)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate ({})", newState);
            lastObservedState.set(newState);
            listener.onNewClusterState(newState.clusterState);
        } else {
            logger.trace("observer: sampled state rejected by predicate ({}). adding listener to ClusterService", newState);
            ObservingContext context = new ObservingContext(listener, changePredicate);
            if (!observingContext.compareAndSet(null, context)) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            clusterService.add(timeoutTimeLeft == null ? null : new TimeValue(timeoutTimeLeft), clusterStateListener);
        }
    }

    /**
     * reset this observer to the give cluster state. Any pending waits will be canceled.
     *
     * @param toState
     */
    public void reset(ClusterState toState) {
        if (observingContext.getAndSet(null) != null) {
            clusterService.remove(clusterStateListener);
        }
        lastObservedState.set(new ObservedState(toState));
    }

    class ObserverClusterStateListener implements TimeoutClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            if (context.changePredicate.apply(event)) {
                if (observingContext.compareAndSet(context, null)) {
                    clusterService.remove(this);
                    ObservedState state = new ObservedState(event.state());
                    logger.trace("observer: accepting cluster state change ({})", state);
                    lastObservedState.set(state);
                    context.listener.onNewClusterState(state.clusterState);
                } else {
                    logger.trace("observer: predicate approved change but observing context has changed - ignoring (new cluster state version [{}])", event.state().version());
                }
            } else {
                logger.trace("observer: predicate rejected change (new cluster state version [{}])", event.state().version());
            }
        }

        @Override
        public void postAdded() {
            ObservingContext context = observingContext.get();
            if (context == null) {
                // No need to remove listener as it is the responsibility of the thread that set observingContext to null
                return;
            }
            ObservedState newState = new ObservedState(clusterService.state());
            ObservedState lastState = lastObservedState.get();
            if (context.changePredicate.apply(lastState.clusterState, lastState.status, newState.clusterState, newState.status)) {
                // double check we're still listening
                if (observingContext.compareAndSet(context, null)) {
                    logger.trace("observer: post adding listener: accepting current cluster state ({})", newState);
                    clusterService.remove(this);
                    lastObservedState.set(newState);
                    context.listener.onNewClusterState(newState.clusterState);
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
                clusterService.remove(this);
                context.listener.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            ObservingContext context = observingContext.getAndSet(null);
            if (context != null) {
                clusterService.remove(this);
                long timeSinceStart = System.currentTimeMillis() - startTime;
                logger.debug("observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStart));
                // update to latest, in case people want to retry
                lastObservedState.set(new ObservedState(clusterService.state()));
                timedOut = true;
                context.listener.onTimeout(timeOutValue);
            }
        }
    }

    public static interface Listener {

        /** called when a new state is observed */
        void onNewClusterState(ClusterState state);

        /** called when the cluster service is closed */
        void onClusterServiceClose();

        void onTimeout(TimeValue timeout);
    }

    public interface ChangePredicate {

        /**
         * a rough check used when starting to monitor for a new change. Called infrequently can be less accurate.
         *
         * @return true if newState should be accepted
         */
        public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus,
                             ClusterState newState, ClusterState.ClusterStateStatus newStatus);

        /**
         * called to see whether a cluster change should be accepted
         *
         * @return true if changedEvent.state() should be accepted
         */
        public boolean apply(ClusterChangedEvent changedEvent);
    }


    public static abstract class ValidationPredicate implements ChangePredicate {

        @Override
        public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus, ClusterState newState, ClusterState.ClusterStateStatus newStatus) {
            if (previousState != newState || previousStatus != newStatus) {
                return validate(newState);
            }
            return false;
        }

        protected abstract boolean validate(ClusterState newState);

        @Override
        public boolean apply(ClusterChangedEvent changedEvent) {
            if (changedEvent.previousState().version() != changedEvent.state().version()) {
                return validate(changedEvent.state());
            }
            return false;
        }
    }

    public static abstract class EventPredicate implements ChangePredicate {
        @Override
        public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus, ClusterState newState, ClusterState.ClusterStateStatus newStatus) {
            return previousState != newState || previousStatus != newStatus;
        }

    }

    static class ObservingContext {
        final public Listener listener;
        final public ChangePredicate changePredicate;

        public ObservingContext(Listener listener, ChangePredicate changePredicate) {
            this.listener = listener;
            this.changePredicate = changePredicate;
        }
    }

    static class ObservedState {
        final public ClusterState clusterState;
        final public ClusterState.ClusterStateStatus status;

        public ObservedState(ClusterState clusterState) {
            this.clusterState = clusterState;
            this.status = clusterState.status();
        }

        @Override
        public String toString() {
            return "version [" + clusterState.version() + "], status [" + status + "]";
        }
    }
}
