/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;

/**
 * Executes an action on an index related to its lifecycle.
 */
public interface LifecycleAction extends ToXContentObject, NamedWriteable {

    /**
     * Checks the current state of the {@link LifecycleAction} and progresses
     * the action towards completion . Note that a {@link LifecycleAction} may
     * require multiple calls to this method before it is complete. Equally this
     * method may do nothing if it needs to wait for something to complete
     * before proceeding.
     * 
     * @param index
     *            the {@link Index} on which to perform the action.
     * @param client
     *            the {@link Client} to use for making changes to the index.
     * @param listener
     *            a {@link Listener} to call when this call completes.
     */
    void execute(Index index, Client client, ClusterService clusterService, Listener listener);

    default boolean indexSurvives() {
        return true;
    }

    /**
     * A callback for when a {@link LifecycleAction} finishes executing
     */
    interface Listener {

        /**
         * Called if the call to
         * {@link LifecycleAction#execute(Index, Client, ClusterService, Listener)}
         * was successful
         * 
         * @param completed
         *            <code>true</code> iff the {@link LifecycleAction} is now
         *            complete and requires no more calls to
         *            {@link LifecycleAction#execute(Index, Client, ClusterService, Listener)
         *            execute(Index, Client, Listener)}.
         */
        void onSuccess(boolean completed);

        /**
         * Called if there was an exception during
         * {@link LifecycleAction#execute(Index, Client, ClusterService, Listener)}.
         * Note that even the call to
         * {@link LifecycleAction#execute(Index, Client, ClusterService, Listener)}
         * may be retried even after this method is called.
         * 
         * @param e
         *            the exception that caused the failure
         */
        void onFailure(Exception e);
    }
}
