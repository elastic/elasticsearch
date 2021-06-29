/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

final class CloseFollowerIndexStep extends AsyncRetryDuringSnapshotActionStep {

    static final String NAME = "close-follower-index";

    CloseFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Boolean> listener) {
        String followerIndex = indexMetadata.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetadata.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followerIndex)
                .masterNodeTimeout(TimeValue.MAX_VALUE);
            getClient().admin().indices().close(closeIndexRequest, ActionListener.wrap(
                r -> {
                    if (r.isAcknowledged() == false) {
                        throw new ElasticsearchException("close index request failed to be acknowledged");
                    }
                    listener.onResponse(true);
                },
                listener::onFailure)
            );
        } else {
            listener.onResponse(true);
        }
    }
}
