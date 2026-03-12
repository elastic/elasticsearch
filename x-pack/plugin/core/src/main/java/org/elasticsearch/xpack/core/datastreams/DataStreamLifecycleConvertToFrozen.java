/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

/**
 * This class encapsulates the steps necessary to convert a data stream backing index to frozen.
 */
public class DataStreamLifecycleConvertToFrozen implements Runnable {

    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleConvertToFrozen.class);

    private final String indexName;
    private final Client client;
    private final ProjectState projectState;

    public DataStreamLifecycleConvertToFrozen(String indexName, Client client, ProjectState projectState) {
        this.indexName = indexName;
        this.client = client;
        this.projectState = projectState;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        // Todo: WIP - only the first step of marking the index read-only is implemented for now,
        // the rest of the steps will be implemented in follow-up PRs
        maybeMarkIndexReadOnly();
        maybeCloneIndex();
        maybeForceMergeIndex();
        maybeTakeSnapshot();
        maybeMountSearchableSnapshot();
    }

    /**
     * Marks the index as read-only by adding a WRITE block, if the block is not already present.
     * This ensures all in-flight writes are completed and flushed to segments before proceeding
     * with the subsequent convert-to-frozen steps.
     *
     * @throws ElasticsearchException if the request to add the write block fails or is not acknowledged
     */
    public void maybeMarkIndexReadOnly() {
        if (isIndexReadOnly(indexName, projectState)) {
            logger.debug("Index [{}] is already marked as read-only, skipping to clone step", indexName);
            return;
        }
        ProjectId projectId = projectState.projectId();

        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(WRITE, indexName).masterNodeTimeout(
            INFINITE_MASTER_NODE_TIMEOUT
        );
        // Force a flush while adding the read-only block to ensure all in-flight writes are completed and written to segments
        addIndexBlockRequest.markVerified(true);

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        addIndexBlock(projectId, addIndexBlockRequest, future);
        future.actionGet();
        logger.debug("DLM successfully marked index [{}] as read-only", indexName);
    }

    public void maybeCloneIndex() {

    }

    public void maybeForceMergeIndex() {

    }

    public void maybeTakeSnapshot() {

    }

    public void maybeMountSearchableSnapshot() {

    }

    private boolean isIndexReadOnly(String indexName, ProjectState projectState) {
        return projectState.blocks().hasIndexBlock(projectState.projectId(), indexName, WRITE.getBlock());
    }

    private void addIndexBlock(ProjectId projectId, AddIndexBlockRequest addIndexBlockRequest, ActionListener<Void> listener) {
        assert addIndexBlockRequest.indices() != null && addIndexBlockRequest.indices().length == 1
            : "DLM should update the index block for one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = addIndexBlockRequest.indices()[0];
        logger.trace("DLM issuing request to add block [{}] for index [{}]", addIndexBlockRequest.getBlock(), targetIndex);
        client.projectClient(projectId)
            .admin()
            .indices()
            .addBlock(addIndexBlockRequest, new AddIndexBlockResponseActionListener(addIndexBlockRequest, targetIndex, listener));
    }

    private static class AddIndexBlockResponseActionListener implements ActionListener<AddIndexBlockResponse> {
        private final AddIndexBlockRequest addIndexBlockRequest;
        private final String targetIndex;
        private final ActionListener<Void> listener;

        private AddIndexBlockResponseActionListener(
            AddIndexBlockRequest addIndexBlockRequest,
            String targetIndex,
            ActionListener<Void> listener
        ) {
            this.addIndexBlockRequest = addIndexBlockRequest;
            this.targetIndex = targetIndex;
            this.listener = listener;
        }

        @Override
        public void onResponse(AddIndexBlockResponse addIndexBlockResponse) {
            if (addIndexBlockResponse.isAcknowledged()) {
                logger.info("DLM successfully added block [{}] for index [{}]", addIndexBlockRequest.getBlock(), targetIndex);
                listener.onResponse(null);
            } else {
                Optional<AddIndexBlockResponse.AddBlockResult> resultForTargetIndex = addIndexBlockResponse.getIndices()
                    .stream()
                    .filter(blockResult -> blockResult.getIndex().getName().equals(targetIndex))
                    .findAny();
                if (resultForTargetIndex.isEmpty()) {
                    // This really should not happen but, if it does, mark as a fail and retry next DLM run
                    logger.trace(
                        "DLM received an unacknowledged response when attempting to add the "
                            + "read-only block to index [{}], but the response didn't contain an explicit result for the index.",
                        targetIndex
                    );
                    listener.onFailure(
                        new ElasticsearchException("request to mark index [" + targetIndex + "] as read-only was not acknowledged")
                    );
                } else if (resultForTargetIndex.get().hasFailures()) {
                    AddIndexBlockResponse.AddBlockResult blockResult = resultForTargetIndex.get();
                    if (blockResult.getException() != null) {
                        listener.onFailure(blockResult.getException());
                    } else {
                        List<AddIndexBlockResponse.AddBlockShardResult.Failure> shardFailures = new ArrayList<>(
                            blockResult.getShards().length
                        );
                        for (AddIndexBlockResponse.AddBlockShardResult shard : blockResult.getShards()) {
                            if (shard.hasFailures()) {
                                shardFailures.addAll(Arrays.asList(shard.getFailures()));
                            }
                        }
                        assert shardFailures.isEmpty() == false
                            : "The block response must have shard failures as the global "
                                + "exception is null. The block result is: "
                                + blockResult;
                        String errorMessage = org.elasticsearch.common.Strings.collectionToDelimitedString(
                            shardFailures.stream().map(org.elasticsearch.common.Strings::toString).collect(Collectors.toList()),
                            ","
                        );
                        listener.onFailure(new ElasticsearchException(errorMessage));
                    }
                } else {
                    listener.onFailure(
                        new ElasticsearchException("request to mark index [" + targetIndex + "] as read-only was not acknowledged")
                    );
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof IndexNotFoundException) {
                // index was already deleted, treat this as a success
                listener.onResponse(null);
                return;
            }
            listener.onFailure(e);
        }
    }
}
