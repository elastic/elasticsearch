/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;

public class DeleteAsyncActionStep extends AsyncActionStep {

    public DeleteAsyncActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    public void performAction(Index index, Listener listener) {
        getClient().admin().indices().prepareDelete(index.getName())
            .execute(ActionListener.wrap(response -> listener.onResponse(true) , listener::onFailure));
    }
}
