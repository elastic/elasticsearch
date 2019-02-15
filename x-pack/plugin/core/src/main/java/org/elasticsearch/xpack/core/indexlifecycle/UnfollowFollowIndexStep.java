/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

final class UnfollowFollowIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "unfollow-follower-index";

    UnfollowFollowIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        UnfollowAction.Request request = new UnfollowAction.Request(followerIndex);
        getClient().execute(UnfollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "unfollow response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }

}
