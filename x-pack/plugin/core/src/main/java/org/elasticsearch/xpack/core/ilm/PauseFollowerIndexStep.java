/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;

final class PauseFollowerIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "pause-follower-index";

    PauseFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        PauseFollowAction.Request request = new PauseFollowAction.Request(followerIndex);
        getClient().execute(PauseFollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "pause follow response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }
}
