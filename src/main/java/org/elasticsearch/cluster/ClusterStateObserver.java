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

import java.util.concurrent.atomic.AtomicBoolean;

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

    volatile ClusterState lastObservedClusterState;
    volatile ClusterState.ClusterStateStatus lastObservedClusterStateStatus;
    private ClusterService clusterService;
    volatile TimeValue timeOutValue;

    // true if a this observer is listening to cluster state changes
    final AtomicBoolean observing = new AtomicBoolean(false);
    volatile ChangePredicate predicate;
    volatile Listener listener;
    volatile long startTime;
    volatile boolean timedOut;

    volatile TimeoutClusterStateListener clusterStateListener = new ObserverClusterStateListener();


    public ClusterStateObserver(ClusterService clusterService, ESLogger logger) {
        this(clusterService, new TimeValue(60000), logger);
    }

    /**
     * @param clusterService
     * @param timeout        a global timeout for this observer. After it has expired the observer
     *                       will fail any existing or new #waitForNextChange calls.
     */
    public ClusterStateObserver(ClusterService clusterService, TimeValue timeout, ESLogger logger) {
        this.timeOutValue = timeout;
        this.clusterService = clusterService;
        this.lastObservedClusterState = clusterService.state();
        this.lastObservedClusterStateStatus = lastObservedClusterState.status();
        this.startTime = System.currentTimeMillis();
        this.logger = logger;
    }

    /** last cluster state observer by this observer. Note that this may not be the current one */
    public ClusterState observedState() {
        return lastObservedClusterState;
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
     * @param changePredicate predicate to indicate when the callback will be called
     * @param timeOutValue    a timeout for waiting. If null the global observer timeout will be used.
     */
    public void waitForNextChange(Listener listener, ChangePredicate changePredicate, @Nullable TimeValue timeOutValue) {

        if (observing.get()) {
            throw new ElasticsearchException("already waiting for a cluster state change");
        }
        long timeoutTimeLeft;
        if (timeOutValue == null) {
            timeOutValue = this.timeOutValue;
            long timeSinceStart = System.currentTimeMillis() - startTime;
            timeoutTimeLeft = timeOutValue.millis() - timeSinceStart;
            if (timeoutTimeLeft <= 0l) {
                // things have timeout while we were busy -> notify
                logger.debug("observer timed out. notifying listener. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStart));
                // update to latest, in case people want to retry
                timedOut = true;
                setObservedState(clusterService.state());
                listener.onTimeout(timeOutValue);
                return;
            }
        } else {
            this.startTime = System.currentTimeMillis();
            this.timeOutValue = timeOutValue;
            timeoutTimeLeft = timeOutValue.millis();
            timedOut = false;
        }

        // sample a new state
        ClusterState newState = clusterService.state();
        ClusterState.ClusterStateStatus newStatus = newState.status();
        if (changePredicate.apply(lastObservedClusterState, lastObservedClusterStateStatus, newState, newStatus)) {
            // good enough, let's go.
            logger.trace("observer: sampled state accepted by predicate (version {})", newState.version());
            setObservedState(newState);
            listener.onNewClusterState(lastObservedClusterState);
        } else {
            logger.trace("observer: sampled state rejected by predicate (version {}). adding listener to ClusterService", newState.version());
            if (observing.getAndSet(true)) {
                throw new ElasticsearchException("already waiting for a cluster state change");
            }
            predicate = changePredicate;
            this.listener = listener;
            clusterService.add(new TimeValue(timeoutTimeLeft), clusterStateListener);
        }
    }

    public void close() {
        if (observing.getAndSet(false)) {
            clusterService.remove(clusterStateListener);
            logger.trace("cluster state observer closed");
        }
    }

    /**
     * reset this observer to the give cluster state. Any pending waits will be canceled.
     *
     * @param toState
     */
    public void reset(ClusterState toState) {
        if (observing.getAndSet(false)) {
            clusterService.remove(clusterStateListener);
        }
        setObservedState(toState);
    }

    // assume lock is acquired
    private void setObservedState(ClusterState state) {
        lastObservedClusterState = state;
        lastObservedClusterStateStatus = state.status();
    }


    class ObserverClusterStateListener implements TimeoutClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            // check if this observer is still active. No need to remove listener as it is the responsibility of the
            // thread that set observing to false
            if (!observing.get()) {
                return;
            }
            if (predicate.apply(event)) {
                if (observing.getAndSet(false)) {
                    clusterService.remove(this);
                    ClusterState state = event.state();
                    logger.trace("observer: accepting cluster state change (version [{}], status [{})", state.version(), state.status());
                    setObservedState(state);
                    listener.onNewClusterState(state);
                }
            }
        }

        @Override
        public void postAdded() {
            // check if this observer is still active. No need to remove listener as it is the responsibility of the
            // thread that set observing to false
            if (!observing.get()) {
                return;
            }
            ClusterState state = clusterService.state();
            if (predicate.apply(lastObservedClusterState, lastObservedClusterStateStatus, state, state.status())) {
                // double check we're still listening
                if (observing.getAndSet(false)) {
                    logger.trace("observer: post adding listener: accepting current cluster state (version [{}], status [{})", state.version(), state.status());
                    clusterService.remove(this);
                    setObservedState(state);
                    listener.onNewClusterState(state);
                }
            }
        }

        @Override
        public void onClose() {
            if (observing.getAndSet(false)) {
                logger.trace("observer: cluster service closed. notifying listener.");
                clusterService.remove(this);
                listener.onClusterServiceClose();
            }
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            if (observing.getAndSet(false)) {
                clusterService.remove(this);
                long timeSinceStart = System.currentTimeMillis() - startTime;
                logger.debug("observer: timeout notification from cluster service. timeout setting [{}], time since start [{}]", timeOutValue, new TimeValue(timeSinceStart));
                // update to latest, in case people want to retry
                setObservedState(clusterService.state());
                timedOut = true;
                listener.onTimeout(timeOutValue);
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
                validate(changedEvent.state());
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
}
