/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;

/**
 * Performs an action which must be performed asynchronously because it may take time to complete.
 */
public abstract class AsyncActionStep extends Step {

    private Client client;

    public AsyncActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey);
        this.client = client;
    }

    protected Client getClient() {
        return client;
    }

    public static TimeValue getMasterTimeout(ClusterState clusterState){
        Objects.requireNonNull(clusterState, "cannot determine master timeout when cluster state is null");
        return LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING.get(clusterState.metadata().settings());
    }

    public boolean indexSurvives() {
        return true;
    }

    public abstract void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                       ClusterStateObserver observer, Listener listener);

    public interface Listener {

        void onResponse(boolean complete);

        void onFailure(Exception e);
    }

}
