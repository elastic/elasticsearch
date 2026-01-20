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
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;

/**
 * This step makes the index read-only when executed
 */
public class ReadOnlyStep implements DlmStep {
    
    private static final Logger logger = getLogger(ReadOnlyStep.class);
    
    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        return projectState.blocks().hasIndexBlock(projectState.projectId(), index.getName(), IndexMetadata.APIBlock.WRITE.getBlock());
    }

    @Override
    public void execute(
        Index index,
        ProjectState projectState,
        ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> transportActionsDeduplicator
    ) {
        var projectId = projectState.projectId();
        var indexName = index.getName();

        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(WRITE, indexName).masterNodeTimeout(TimeValue.MAX_VALUE);
        transportActionsDeduplicator.executeOnce(
            Tuple.tuple(projectId, addIndexBlockRequest),
            new DataStreamLifecycleService.ErrorRecordingActionListener(
                TransportAddIndexBlockAction.TYPE.name(),
                projectId,
                indexName,
                errorStore,
                Strings.format("Data stream lifecycle service encountered an error trying to mark index [%s] as readonly", indexName),
                signallingErrorRetryInterval
            ),
            (req, reqListener) -> addIndexBlock(projectId, addIndexBlockRequest, reqListener)
        );
    }

    private void addIndexBlock(ProjectId projectId, AddIndexBlockRequest addIndexBlockRequest, ActionListener<Void> listener) {
        assert addIndexBlockRequest.indices() != null && addIndexBlockRequest.indices().length == 1
            : "Data stream lifecycle service updates the index block for one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = addIndexBlockRequest.indices()[0];
        logger.trace(
            "Data stream lifecycle service issues request to add block [{}] for index [{}]",
            addIndexBlockRequest.getBlock(),
            targetIndex
        );
        client.projectClient(projectId).admin().indices().addBlock(addIndexBlockRequest, new ActionListener<>() {
            @Override
            public void onResponse(AddIndexBlockResponse addIndexBlockResponse) {
                if (addIndexBlockResponse.isAcknowledged()) {
                    logger.info(
                        "Data stream lifecycle service successfully added block [{}] for index index [{}]",
                        addIndexBlockRequest.getBlock(),
                        targetIndex
                    );
                    listener.onResponse(null);
                } else {
                    Optional<AddIndexBlockResponse.AddBlockResult> resultForTargetIndex = addIndexBlockResponse.getIndices()
                        .stream()
                        .filter(blockResult -> blockResult.getIndex().getName().equals(targetIndex))
                        .findAny();
                    if (resultForTargetIndex.isEmpty()) {
                        // blimey
                        // this is weird, we don't have a result for our index, so let's treat this as a success and the next DSL run will
                        // check if we need to retry adding the block for this index
                        logger.trace(
                            "Data stream lifecycle service received an unacknowledged response when attempting to add the "
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
                    errorStore.clearRecordedError(projectId, targetIndex);
                    listener.onResponse(null);
                    return;
                }

                listener.onFailure(e);
            }
        });
    }

    @Override
    public String stepName() {
        return "Make Index Read Only";
    }
}
