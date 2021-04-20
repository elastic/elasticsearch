/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

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
     *
     * @param source information where the cluster state came from
     * @param clusterStateSupplier the cluster state supplier which provides the latest cluster state to apply
     * @param listener callback that is invoked after cluster state is applied
     */
    void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener);

    /**
     * Listener for results of cluster state application
     */
    interface ClusterApplyListener {
        /**
         * Called on successful cluster state application
         * @param source information where the cluster state came from
         */
        default void onSuccess(String source) {
        }

        /**
         * Called on failure during cluster state application
         * @param source information where the cluster state came from
         * @param e exception that occurred
         */
        void onFailure(String source, Exception e);
    }
}
