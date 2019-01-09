/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.Client;

final class OpenFollowerIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "open-follower-index";

    OpenFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        OpenIndexRequest request = new OpenIndexRequest(followerIndex);
        getClient().admin().indices().open(request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() :  "open index response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }
}
