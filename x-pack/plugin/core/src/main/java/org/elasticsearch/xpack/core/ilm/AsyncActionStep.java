/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;

/**
 * Performs an action which must be performed asynchronously because it may take time to complete.
 */
public abstract class AsyncActionStep extends Step {

    private final Client client;

    public AsyncActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey);
        this.client = client;
    }

    // For testing only
    @Nullable
    Client getClient() {
        return client;
    }

    protected Client getClient(ProjectId projectId) {
        return client.projectClient(projectId);
    }

    public boolean indexSurvives() {
        return true;
    }

    public abstract void performAction(
        IndexMetadata indexMetadata,
        ProjectState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    );
}
