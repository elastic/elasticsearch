/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

/**
 * Marks an index as read-only, by setting a {@link org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock#WRITE } block on the index.
 */
public class ReadOnlyStep extends AsyncActionStep {
    public static final String NAME = "readonly";
    private final boolean markVerified;

    /**
     * @param markVerified whether the index should be marked verified after becoming read-only, ensuring that N-2 is supported without
     *                     manual intervention. Should be set to true when the read-only block is not temporary.
     */
    public ReadOnlyStep(StepKey key, StepKey nextStepKey, Client client, boolean markVerified) {
        super(key, nextStepKey, client);
        this.markVerified = markVerified;
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
                TransportAddIndexBlockAction.TYPE,
                new AddIndexBlockRequest(WRITE, indexMetadata.getIndex().getName()).masterNodeTimeout(TimeValue.MAX_VALUE)
                    .markVerified(markVerified),
                listener.delegateFailureAndWrap((l, response) -> {
                    if (response.isAcknowledged() == false) {
                        throw new ElasticsearchException("read only add block index request failed to be acknowledged");
                    }
                    l.onResponse(null);
                })
            );
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
