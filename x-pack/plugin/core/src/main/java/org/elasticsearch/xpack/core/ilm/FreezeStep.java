/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;

/**
 * Freezes an index.
 */
@Deprecated // To be removed in 9.0
public class FreezeStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "freeze";

    public FreezeStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, ActionListener<Void> listener) {
        // Deprecated in 7.x, the freeze action is a noop in 8.x, so immediately return here
        listener.onResponse(null);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
