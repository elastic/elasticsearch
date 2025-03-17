/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

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
    public void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentState, ActionListener<Void> listener) {
        String policyName = indexMetadata.getLifecyclePolicyName();
        String indexName = indexMetadata.getIndex().getName();
        IndexAbstraction indexAbstraction = currentState.metadata().getProject().getIndicesLookup().get(indexName);
        assert indexAbstraction != null : "invalid cluster metadata. index [" + indexName + "] was not found";
        DataStream dataStream = indexAbstraction.getParentDataStream();

        if (dataStream != null) {
            Index failureStoreWriteIndex = dataStream.getWriteFailureIndex();
            boolean isFailureStoreWriteIndex = failureStoreWriteIndex != null && indexName.equals(failureStoreWriteIndex.getName());

            // using index name equality across this if/else branch as the UUID of the index might change via restoring a data stream
            // with one index from snapshot
            if (dataStream.getIndices().size() == 1
                && isFailureStoreWriteIndex == false
                && dataStream.getWriteIndex().getName().equals(indexName)) {
                // This is the last backing index in the data stream, and it's being deleted because the policy doesn't have a rollover
                // phase. The entire stream needs to be deleted, because we can't have an empty list of data stream backing indices.
                // We do this even if there are multiple failure store indices because otherwise we would never delete the index.
                DeleteDataStreamAction.Request deleteReq = new DeleteDataStreamAction.Request(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    dataStream.getName()
                );
                getClient().execute(
                    DeleteDataStreamAction.INSTANCE,
                    deleteReq,
                    listener.delegateFailureAndWrap((l, response) -> l.onResponse(null))
                );
                return;
            } else if (isFailureStoreWriteIndex || dataStream.getWriteIndex().getName().equals(indexName)) {
                String errorMessage = Strings.format(
                    "index [%s] is the%s write index for data stream [%s]. "
                        + "stopping execution of lifecycle [%s] as a data stream's write index cannot be deleted. manually rolling over the"
                        + " index will resume the execution of the policy as the index will not be the data stream's write index anymore",
                    indexName,
                    isFailureStoreWriteIndex ? " failure store" : "",
                    dataStream.getName(),
                    policyName
                );
                logger.debug(errorMessage);
                listener.onFailure(new IllegalStateException(errorMessage));
                return;
            }
        }

        getClient().admin()
            .indices()
            .delete(
                new DeleteIndexRequest(indexName).masterNodeTimeout(TimeValue.MAX_VALUE),
                listener.delegateFailureAndWrap((l, response) -> l.onResponse(null))
            );
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
