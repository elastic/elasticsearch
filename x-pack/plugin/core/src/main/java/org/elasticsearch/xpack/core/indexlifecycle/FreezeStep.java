/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.action.TransportFreezeIndexAction;

/**
 * Freezes an index.
 */
public class FreezeStep extends AsyncActionStep {
    public static final String NAME = "freeze";

    public FreezeStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        getClient().admin().indices().execute(TransportFreezeIndexAction.FreezeIndexAction.INSTANCE,
            new TransportFreezeIndexAction.FreezeRequest(indexMetaData.getIndex().getName()),
            ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }
}
