/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * A utility class for interacting with a project state.
 * Analogous to {@link ClusterStateObserver} but scoped to a single project
 */
public class ProjectStateObserver {

    private final ProjectId projectId;
    private final ClusterStateObserver clusterObserver;

    public ProjectStateObserver(
        ProjectState initialState,
        ClusterService clusterService,
        @Nullable TimeValue timeout,
        Logger logger,
        ThreadContext contextHolder
    ) {
        this(initialState.projectId(), initialState.cluster(), clusterService, timeout, logger, contextHolder);
    }

    private ProjectStateObserver(
        ProjectId projectId,
        ClusterState initialClusterState,
        ClusterService clusterService,
        @Nullable TimeValue timeout,
        Logger logger,
        ThreadContext contextHolder
    ) {
        this.projectId = projectId;
        this.clusterObserver = new ClusterStateObserver(initialClusterState, clusterService, timeout, logger, contextHolder);
    }

    public void waitForNextChange(ProjectStateObserver.Listener listener, @Nullable TimeValue timeOutValue) {
        this.clusterObserver.waitForNextChange(new ListenerAdapter(listener), timeOutValue);
    }

    public boolean isTimedOut() {
        return clusterObserver.isTimedOut();
    }

    /**
     * Obtain the most recent {@link ClusterApplierService#state() applied cluster state} and then invoke either
     * {@link ProjectStateObserver.Listener#onProjectStateChange(ProjectState)} or
     * {@link ProjectStateObserver.Listener#onProjectMissing(ProjectId, ClusterState)} on the provided {@code listener}
     */
    public void observeLastAppliedState(ProjectStateObserver.Listener listener) {
        applyProjectState(clusterObserver.setAndGetObservedState(), listener);
    }

    private void applyProjectState(ClusterState clusterState, ProjectStateObserver.Listener listener) {
        if (clusterState.metadata().hasProject(projectId)) {
            listener.onProjectStateChange(clusterState.projectState(projectId));
        } else {
            listener.onProjectMissing(projectId, clusterState);
        }
    }

    public interface Listener {
        /**
         * called when the project state has (or might have) changed.
         */
        void onProjectStateChange(ProjectState projectState);

        /**
         * called when the cluster metadata does not contain the project
         */
        void onProjectMissing(ProjectId projectId, ClusterState clusterState);

        /** called when the cluster service is closed */
        void onClusterServiceClose();

        /**
         * Called when the {@link ClusterStateObserver} times out while waiting for a new matching cluster state if a timeout is
         * used when creating the observer. Upon timeout, {@code onTimeout} is called on the GENERIC threadpool.
         */
        void onTimeout(TimeValue timeout);

    }

    private class ListenerAdapter implements ClusterStateObserver.Listener {

        private final Listener delegate;

        private ListenerAdapter(Listener delegate) {
            this.delegate = delegate;
        }

        @Override
        public final void onNewClusterState(ClusterState clusterState) {
            applyProjectState(clusterState, delegate);
        }

        @Override
        public void onClusterServiceClose() {
            delegate.onClusterServiceClose();
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            delegate.onTimeout(timeout);
        }
    }

}
