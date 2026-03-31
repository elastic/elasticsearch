/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * A {@link ClusterStateApplier} which delegates asynchronously to another {@link ClusterStateApplier}, and keeps track of
 * {@link ClusterState} instances to be applied in the future, allowing callers to subscribe listeners which are notified when the current
 * {@link ClusterState} has eventually been fully applied.
 */
public class AsyncClusterStateApplier implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(AsyncClusterStateApplier.class);

    private final Executor executor;
    private final ClusterStateApplier applier;

    @Nullable // before calling setInitialState
    private ClusterState appliedState;

    /**
     * When {@code false}, the applier is idle: {@link #pendingState} is {@code null}, {@link #listeners} is complete, and no application
     * is enqueued on {@link #executor}. When {@code true}, the applier is active ({@link #applierRunnable} is either enqueued or running).
     */
    private boolean isApplying;

    /**
     * Records the {@link ClusterState} to apply after the current application is finished.
     */
    @Nullable // if no states pending (maybe currently applying a state, but no further states to apply)
    private ClusterState pendingState;

    /**
     * Accumulates listeners to notify at the end of the application of the last cluster state provided to {@link #applyClusterState} before
     * the invocation of this method.
     * <p>
     * Complete if the applier is idle.
     * <p>
     * If the applier is active but {@link #pendingState} is {@code null} then it will be completed when the current application completes.
     * <p>
     * If the applier is active and {@link #pendingState} is not {@code null} then it will be completed when the <i>next</i> application
     * completes.
     */
    private volatile SubscribableListener<Void> listeners = SubscribableListener.nullSuccess();

    public AsyncClusterStateApplier(ClusterStateApplier applier, Executor executor) {
        this.executor = executor;
        this.applier = applier;
    }

    /**
     * Subscribe the given listener to the active cluster state applications, such that the listener will be completed after the application
     * of all cluster states which were passed to {@link #applyClusterState} before this method was called.
     */
    public void awaitCurrentStateApplication(ActionListener<Void> listener) {
        listeners.addListener(listener);
    }

    /**
     * Asynchronously apply a new {@link ClusterState}. Listeners passed to {@link #awaitCurrentStateApplication} after this method is
     * called will be completed after the given {@link ClusterState} has been applied.
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final var oldState = Objects.requireNonNull(event.previousState());
        final var newState = Objects.requireNonNull(event.state());

        synchronized (this) {
            if (appliedState == null) {
                // first state application
                appliedState = oldState;
            }

            if (isApplying) {
                if (pendingState == null) {
                    // first state in a new pending batch so start accumulating listeners afresh
                    listeners = new SubscribableListener<>();
                }
                pendingState = newState;
                return;
            }

            // idle, so start applying the state
            assert listeners.isSuccess();
            isApplying = true;
            pendingState = newState;
            listeners = new SubscribableListener<>();
        }

        executor.execute(applierRunnable);
    }

    private final AbstractRunnable applierRunnable = new AbstractRunnable() {
        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected exception applying cluster state to indices", e);
            assert false : e;
        }

        @Override
        protected void doRun() {
            final ClusterState stateToApply;
            final SubscribableListener<Void> applyListener;
            synchronized (AsyncClusterStateApplier.this) {
                assert isApplying;
                stateToApply = pendingState;
                assert stateToApply != null;
                pendingState = null;
                applyListener = listeners;
            }

            // NB this is effectively single-threaded (there's a happens-before relationship between all pairs of invocations) so there is
            // no need to synchronize access to appliedState
            assert appliedState != null : "must be initialized";

            final var source = Strings.format(
                "async update state from version [%d] to version [%d]",
                appliedState.version(),
                stateToApply.version()
            );

            logger.info("--> waiting before async applying [{}]", source);
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("nocommit", e);
            }
            logger.info("--> async applying [{}]", source);
            applier.applyClusterState(new ClusterChangedEvent(source, stateToApply, appliedState));
            logger.info("--> done async applying [{}]", source);
            appliedState = stateToApply;

            assert applyListener.isDone() == false;
            applyListener.onResponse(null);

            synchronized (AsyncClusterStateApplier.this) {
                if (pendingState == null) {
                    assert listeners.isSuccess();
                    isApplying = false;
                    return;
                }
            }

            executor.execute(applierRunnable);
        }
    };
}
