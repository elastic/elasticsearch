/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;

/**
 * Invokes a open step on a single index.
 */

final class OpenIndexStep extends AsyncActionStep {

    static final String NAME = "open-index";

    OpenIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentClusterState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            OpenIndexRequest request = new OpenIndexRequest(indexMetadata.getIndex().getName()).masterNodeTimeout(TimeValue.MAX_VALUE);
            getClient().admin().indices().open(request, listener.delegateFailureAndWrap((l, openIndexResponse) -> {
                if (openIndexResponse.isAcknowledged() == false) {
                    throw new ElasticsearchException("open index request failed to be acknowledged");
                }
                l.onResponse(null);
            }));

        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
