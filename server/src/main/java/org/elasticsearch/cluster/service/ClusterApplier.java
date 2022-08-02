/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

import java.util.function.Supplier;

public interface ClusterApplier {
    /**
     * Sets the initial state for this applier. Should only be called once.
     * @param initialState the initial state to set
     */
    void setInitialState(ClusterState initialState);

    /**
     * Method to invoke when a new cluster state is available to be applied
     *  @param source information where the cluster state came from
     * @param clusterStateSupplier the cluster state supplier which provides the latest cluster state to apply
     * @param listener notified after cluster state is applied. The implementation must not throw exceptions: an exception thrown by this
     *                 listener is logged by the cluster applier service at {@code ERROR} level and otherwise ignored, except in tests where
     *                 it raises an {@link AssertionError}. If log-and-ignore is the right behaviour then implementations must do so
     *                 themselves, typically using a more specific logger and at a less dramatic log level.
     */
    void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ActionListener<Void> listener);

    ClusterApplierRecordingService.Stats getStats();

}
