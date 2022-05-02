/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

/**
 * Marks an index as read-only, by setting a {@link org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock#WRITE } block on the index.
 */
public class ReadOnlyStep extends AsyncActionStep {
    public static final String NAME = "readonly";

    public ReadOnlyStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        getClient().admin()
            .indices()
            .execute(
                AddIndexBlockAction.INSTANCE,
                new AddIndexBlockRequest(WRITE, indexMetadata.getIndex().getName()).masterNodeTimeout(TimeValue.MAX_VALUE),
                ActionListener.wrap(response -> {
                    if (response.isAcknowledged() == false) {
                        throw new ElasticsearchException("read only add block index request failed to be acknowledged");
                    }
                    listener.onResponse(null);
                }, listener::onFailure)
            );
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
