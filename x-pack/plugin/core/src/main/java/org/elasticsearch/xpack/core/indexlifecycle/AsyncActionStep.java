/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;

public abstract class AsyncActionStep extends Step {

    private Client client;

    public AsyncActionStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey);
        this.client = client;
    }

    protected Client getClient() {
        return client;
    }

    public abstract void performAction(Index index, Listener listener);

    public static interface Listener {

        void onResponse(boolean complete);

        void onFailure(Exception e);
    }

}
