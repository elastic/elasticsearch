/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.client.Client;

final class CloseFollowerIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "close-follower-index";

    CloseFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followerIndex);
        getClient().admin().indices().close(closeIndexRequest, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "close index response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure)
        );
    }
}
