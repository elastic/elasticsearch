/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

/**
 * Deletes a single index.
 */
public class DeleteStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "delete";

    public DeleteStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performDuringNoSnapshot(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        getClient().admin().indices()
            .delete(new DeleteIndexRequest(indexMetaData.getIndex().getName()),
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }
}
