/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

final class CloseFollowerIndexStep extends AsyncRetryDuringSnapshotActionStep {

    static final String NAME = "close-follower-index";

    CloseFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, Listener listener) {
        String followerIndex = indexMetadata.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetadata.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followerIndex)
                .masterNodeTimeout(getMasterTimeout(currentClusterState));
            getClient().admin().indices().close(closeIndexRequest, ActionListener.wrap(
                r -> {
                    assert r.isAcknowledged() : "close index response is not acknowledged";
                    listener.onResponse(true);
                },
                listener::onFailure)
            );
        } else {
            listener.onResponse(true);
        }
    }
}
