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
import java.util.Random;
import java.util.concurrent.Executor;

/// A [ClusterStateApplier] which delegates asynchronously to another [ClusterStateApplier], and keeps track of
/// [ClusterState] instances to be applied in the future, allowing callers to subscribe listeners which are notified when the current
/// [ClusterState] has eventually been fully applied.
public class AsyncClusterStateApplier implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(AsyncClusterStateApplier.class);

    private final Executor executor;
    private final ClusterStateApplier applier;

    @Nullable // before calling setInitialState
    private volatile ClusterState appliedState;

    /// When `false`, the applier is idle: [#pendingState] is `null`, [#listeners] is complete, and no application
    /// is enqueued on [#executor]. When `true`, the applier is active ([#applierRunnable] is either enqueued or running).
    private boolean isApplying;

    /// Records the [ClusterState] to apply after the current application is finished.
    @Nullable // if no states pending (maybe currently applying a state, but no further states to apply)
    private ClusterState pendingState;

    /// Accumulates listeners to notify at the end of the application of the last cluster state provided to [#applyClusterState] before
    /// the invocation of this method.
    ///
    /// Complete if the applier is idle.
    /// If the applier is active but [#pendingState] is `null` then it will be completed when the current application completes.
    /// If the applier is active and [#pendingState] is not `null` then it will be completed when the _next_ application completes.
    private volatile SubscribableListener<Void> listeners = SubscribableListener.nullSuccess();

    public AsyncClusterStateApplier(ClusterStateApplier applier, Executor executor) {
        this.executor = executor;
        this.applier = applier;
    }

    /// Returns the version of the last [ClusterState] fully applied by the delegate applier, or -1 if none yet.
    public long appliedStateVersion() {
        final var state = appliedState;
        return state == null ? -1L : state.version();
    }

    /// Subscribe the given listener to the active cluster state applications, such that the listener will be completed
    /// after the application of all cluster states which were passed to [#applyClusterState] before this method was called.
    public void awaitCurrentStateApplication(ActionListener<Void> listener) {
        listeners.addListener(listener);
    }

    /// Asynchronously apply a new [ClusterState]. Listeners passed to [#awaitCurrentStateApplication] after this method is
    /// called will be completed after the given [ClusterState] has been applied.
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final var oldState = Objects.requireNonNull(event.previousState());
        final var newState = Objects.requireNonNull(event.state());
        logger.info("--> new state to apply: old [{}] new [{}]", oldState.version(), newState.version());

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
                logger.info("--> setting new pending state to [{}] while applying from [{}]", newState.version(), appliedState.version());
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

            try {
                logger.info("--> waiting before async applying [{}]", source);
                Thread.sleep(new Random().nextInt(50));
                logger.info("--> async applying [{}]", source);

                // TODO: could this actually throw? Outside of test assertions.
                // If not, we can remove the exception handling below once all tests are fixed
                applier.applyClusterState(new ClusterChangedEvent(source, stateToApply, appliedState));
                logger.info("--> done async applying [{}]", source);
                appliedState = stateToApply;

                // Complete `applyListener` before the synchronized block so `listeners.isSuccess()` holds when we check it.
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
            } catch (Throwable t) {
                logger.error(() -> "failed to apply [" + source + "]", t);
                if (t instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                // Drain any pending-batch listener.
                final SubscribableListener<Void> stagedListeners;
                synchronized (AsyncClusterStateApplier.this) {
                    stagedListeners = (listeners != applyListener) ? listeners : null;
                    listeners = SubscribableListener.nullSuccess();
                    pendingState = null;
                    isApplying = false;
                }

                final var asException = t instanceof Exception ex ? ex : new RuntimeException(t);
                if (applyListener.isDone() == false) {
                    applyListener.onFailure(asException);
                }
                if (stagedListeners != null) {
                    stagedListeners.onFailure(asException);
                }

                throw new AssertionError("do-not-forget-to-remove-this", t);
            }
        }
    };
}
