/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;

/**
 * Performs an action which must be performed asynchronously because it may take time to complete.
 */
public abstract class AsyncActionStep extends Step {

    private final Client client;

    public AsyncActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey);
        this.client = client;
    }

    protected Client getClient() {
        return client;
    }

    public boolean indexSurvives() {
        return true;
    }

    public abstract void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                       ClusterStateObserver observer, ActionListener<Boolean> listener);
}
