/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

/**
 * This step makes the index read-only when executed
 */
public class ReadOnlyStep implements DlmStep {

    private static final Logger logger = getLogger(ReadOnlyStep.class);

    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        return projectState.blocks().hasIndexBlock(projectState.projectId(), index.getName(), WRITE.getBlock());
    }

    @Override
    public void execute(DlmStepContext stepContext) {
        ProjectId projectId = stepContext.projectId();
        String indexName = stepContext.indexName();

        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(WRITE, indexName).masterNodeTimeout(
            INFINITE_MASTER_NODE_TIMEOUT
        );
        // Force a flush while adding the read-only block to ensure all in-flight writes are completed and written to segments
        addIndexBlockRequest.markVerified(true);

        stepContext.executeDeduplicatedRequest(
            TransportAddIndexBlockAction.TYPE.name(),
            addIndexBlockRequest,
            Strings.format("DLM service encountered an error trying to mark index [%s] as readonly", indexName),
            (req, reqListener) -> addIndexBlock(projectId, addIndexBlockRequest, reqListener, stepContext)
        );
    }

    private void addIndexBlock(
        ProjectId projectId,
        AddIndexBlockRequest addIndexBlockRequest,
        ActionListener<Void> listener,
        DlmStepContext stepContext
    ) {
        assert addIndexBlockRequest.indices() != null && addIndexBlockRequest.indices().length == 1
            : "DLM should update the index block for one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = addIndexBlockRequest.indices()[0];
        logger.trace("DLM issuing request to add block [{}] for index [{}]", addIndexBlockRequest.getBlock(), targetIndex);
        stepContext.client()
            .projectClient(projectId)
            .admin()
            .indices()
            .addBlock(
                addIndexBlockRequest,
                new AddIndexBlockResponseActionListener(addIndexBlockRequest, targetIndex, listener, stepContext, projectId)
            );
    }

    @Override
    public String stepName() {
        return "Make Index Read Only";
    }

    private static class AddIndexBlockResponseActionListener implements ActionListener<AddIndexBlockResponse> {
        private final AddIndexBlockRequest addIndexBlockRequest;
        private final String targetIndex;
        private final ActionListener<Void> listener;
        private final DlmStepContext stepContext;
        private final ProjectId projectId;

        private AddIndexBlockResponseActionListener(
            AddIndexBlockRequest addIndexBlockRequest,
            String targetIndex,
            ActionListener<Void> listener,
            DlmStepContext stepContext,
            ProjectId projectId
        ) {
            this.addIndexBlockRequest = addIndexBlockRequest;
            this.targetIndex = targetIndex;
            this.listener = listener;
            this.stepContext = stepContext;
            this.projectId = projectId;
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
                logger.trace("Clearing recorded error for index [{}] because the index was deleted", targetIndex);
                stepContext.errorStore().clearRecordedError(projectId, targetIndex);
                listener.onResponse(null);
                return;
            }

            listener.onFailure(e);
        }
    }
}
