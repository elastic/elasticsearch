/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

/**
 * Freezes an index.
 */
public class FreezeStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "freeze";

    public FreezeStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, ActionListener<Void> listener) {
        getClient().admin().indices().execute(FreezeIndexAction.INSTANCE,
            new FreezeRequest(indexMetadata.getIndex().getName()).masterNodeTimeout(TimeValue.MAX_VALUE),
            ActionListener.wrap(response -> {
                if (response.isAcknowledged() == false) {
                    throw new ElasticsearchException("freeze index request failed to be acknowledged");
                }
                listener.onResponse(null);
            }, listener::onFailure));
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
