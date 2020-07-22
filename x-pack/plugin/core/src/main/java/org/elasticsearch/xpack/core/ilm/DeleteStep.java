/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.Locale;

/**
 * Deletes a single index.
 */
public class DeleteStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "delete";
    private static final Logger logger = LogManager.getLogger(DeleteStep.class);

    public DeleteStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, Listener listener) {
        String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        String indexName = indexMetadata.getIndex().getName();
        IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(indexName);
        assert indexAbstraction != null : "invalid cluster metadata. index [" + indexName + "] was not found";
        IndexAbstraction.DataStream dataStream = indexAbstraction.getParentDataStream();

        if (dataStream != null) {
            assert dataStream.getWriteIndex() != null : dataStream.getName() + " has no write index";
            if (dataStream.getWriteIndex().getIndex().getName().equals(indexName)) {
                String errorMessage = String.format(Locale.ROOT, "index [%s] is the write index for data stream [%s]. " +
                        "stopping execution of lifecycle [%s] as a data stream's write index cannot be deleted. manually rolling over the" +
                        " index will resume the execution of the policy as the index will not be the data stream's write index anymore",
                    indexName, dataStream.getName(), policyName);
                logger.debug(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
        }

        getClient().admin().indices()
            .delete(new DeleteIndexRequest(indexName).masterNodeTimeout(getMasterTimeout(currentState)),
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
