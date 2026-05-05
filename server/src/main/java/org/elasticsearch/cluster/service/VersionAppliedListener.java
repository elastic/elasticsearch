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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;

import java.util.function.Consumer;

/// A [TimeoutClusterStateListener] that notifies a listener once a specific cluster state version has been published.
///
/// Register this listener via [ClusterApplierService#addTimeoutListener]. [#postAdded] is called
/// immediately after registration, so the version check is always race-free even if the target state was
/// published before [#addTimeoutListener] was called.
///
/// Pass `null` as the timeout to [ClusterApplierService#addTimeoutListener] — this listener does not support
/// timeouts and will log an error and assert if [#onTimeout] is called.
/// On node close the listener is failed with [NodeClosedException].
public final class VersionAppliedListener implements TimeoutClusterStateListener {

    private static final Logger logger = LogManager.getLogger(VersionAppliedListener.class);

    private final long clusterStateVersion;
    private final ClusterService clusterService;
    @Nullable
    private final Consumer<Runnable> cancelSubscriber;
    private final ActionListener<Void> listener;

    /// @param clusterStateVersion the minimum published cluster state version to wait for
    /// @param clusterService      used to read the current state and to remove this listener
    /// @param cancelSubscriber    if non-null, called with a removal runnable when the version is not yet
    ///                            reached; the caller can invoke that runnable to cancel the wait early
    ///                            (e.g., on task cancellation)
    /// @param listener            completed with `null` once the version is published, or with a failure
    ///                            on node close
    public VersionAppliedListener(
        long clusterStateVersion,
        ClusterService clusterService,
        @Nullable Consumer<Runnable> cancelSubscriber,
        ActionListener<Void> listener
    ) {
        this.clusterStateVersion = clusterStateVersion;
        this.clusterService = clusterService;
        this.cancelSubscriber = cancelSubscriber;
        this.listener = listener;
    }

    @Override
    public void postAdded() {
        if (clusterService.state().version() >= clusterStateVersion) {
            removeListener();
            listener.onResponse(null);
        } else if (cancelSubscriber != null) {
            cancelSubscriber.accept(this::removeListener);
        }
    }

    private void removeListener() {
        clusterService.getClusterApplierService().removeTimeoutListener(this);
    }

    @Override
    public void onClose() {
        removeListener();
        listener.onFailure(new NodeClosedException(clusterService.localNode()));
    }

    @Override
    public void onTimeout(TimeValue timeout) {
        logger.error("no timeout configured");
        assert false : "no timeout configured";
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().version() >= clusterStateVersion) {
            removeListener();
            listener.onResponse(null);
        }
    }
}
