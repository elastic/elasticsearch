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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;

import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * This step swaps the old backing index with the new frozen index (a partially mounted searchable snapshot)
 * in the data stream, then deletes the old backing index. The swap is performed atomically via a single
 * {@link ModifyDataStreamsAction} request containing both a remove and add backing index action.
 */
public class CleanupStep implements DlmStep {

    private static final Logger logger = getLogger(CleanupStep.class);
    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);

    public static final String DLM_FROZEN_INDEX_PREFIX = "dlm-frozen-";

    /**
     * Determines if the step has been completed for the given index and project state.
     * The step is complete when neither the old index nor its clone exist and the frozen index is part of a data stream.
     *
     * @param index        The old index to check.
     * @param projectState The current project state.
     * @return true if both the old index and clone have been deleted and the frozen index is part of a data stream.
     */
    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        ProjectMetadata projectMetadata = projectState.metadata();
        if (projectMetadata.indices().containsKey(index.getName())) {
            return false;
        }
        String cloneIndexName = CloneStep.getDLMCloneIndexName(index.getName());
        if (projectMetadata.indices().containsKey(cloneIndexName)) {
            return false;
        }
        String frozenIndexName = getFrozenIndexName(index.getName());
        return resolveDataStreamName(frozenIndexName, projectMetadata) != null;
    }

    /**
     * Executes the cleanup step. Swaps the old backing index with the frozen index and deletes it atomically.
     * @param stepContext The context and resources for executing the step.
     */
    @Override
    public void execute(DlmStepContext stepContext) {
        String indexName = stepContext.indexName();
        String frozenIndexName = getFrozenIndexName(indexName);
        ProjectMetadata projectMetadata = stepContext.projectState().metadata();
        String dataStreamName = resolveDataStreamName(indexName, projectMetadata);

        if (dataStreamName == null) {
            // If the old backing index is not part of a data stream, there's no need to swap for the searchable snapshot
            logger.warn("Index [{}] is not part of a data stream, skipping DLM Cleanup Step", indexName);
            return;
        }

        maybeSwapBackingIndexInDataStream(dataStreamName, indexName, frozenIndexName, stepContext);
    }

    /**
     * A human-readable name for the step.
     *
     * @return The step name.
     */
    @Override
    public String stepName() {
        return "Clean Up Backing Index";
    }

    @Override
    public List<String> possibleOutputIndexNamePatterns(String indexName) {
        return List.of(getFrozenIndexName(indexName));
    }

    /**
     * Returns the frozen index name for the given original index name.
     *
     * @param originalName the original backing index name
     * @return the frozen index name with the {@link #DLM_FROZEN_INDEX_PREFIX} prefix
     */
    static String getFrozenIndexName(String originalName) {
        return DLM_FROZEN_INDEX_PREFIX + originalName;
    }

    /**
     * Resolves the parent data stream name for the given index.
     *
     * @param indexName       the index name to look up
     * @param projectMetadata the project metadata containing the indices lookup
     * @return the data stream name, or {@code null} if the index is not part of a data stream
     */
    static String resolveDataStreamName(String indexName, ProjectMetadata projectMetadata) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(indexName);
        if (indexAbstraction == null) {
            return null;
        }
        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null ? parentDataStream.getName() : null;
    }

    /**
     * Swaps a backing index in a data stream by issuing a {@link ModifyDataStreamsAction} request
     * with a remove action for the old index and an add action for the new frozen index. This is
     * collapsed into a single cluster state update, making the operation atomic.
     *
     * @param dataStreamName the name of the data stream
     * @param oldIndex       the old backing index to remove
     * @param newIndex       the new frozen index to add
     * @param stepContext    the DLM step context
     */
    static void maybeSwapBackingIndexInDataStream(String dataStreamName, String oldIndex, String newIndex, DlmStepContext stepContext) {
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(
            TimeValue.MAX_VALUE,
            TimeValue.MAX_VALUE,
            List.of(
                DataStreamAction.removeBackingIndex(dataStreamName, oldIndex),
                DataStreamAction.addBackingIndex(dataStreamName, newIndex)
            )
        );

        stepContext.executeDeduplicatedRequest(
            ModifyDataStreamsAction.NAME,
            request,
            Strings.format(
                "DLM service encountered an error trying to swap backing index [%s] with [%s] in data stream [%s]",
                oldIndex,
                newIndex,
                dataStreamName
            ),
            (req, reqListener) -> {
                logger.debug(
                    "DLM issuing request to swap backing index [{}] with [{}] in data stream [{}]",
                    oldIndex,
                    newIndex,
                    dataStreamName
                );
                stepContext.client()
                    .projectClient(stepContext.projectId())
                    .execute(ModifyDataStreamsAction.INSTANCE, request, ActionListener.wrap(resp -> {
                        if (resp.isAcknowledged()) {
                            logger.info(
                                "DLM successfully swapped backing index [{}] with [{}] in data stream [{}]",
                                oldIndex,
                                newIndex,
                                dataStreamName
                            );
                            // Delete the old backing index and any clone index now that the swap is complete
                            maybeDeleteIndices(oldIndex, stepContext);
                            reqListener.onResponse(null);
                        } else {
                            reqListener.onFailure(
                                new ElasticsearchException(
                                    Strings.format(
                                        "DLM failed to acknowledge swap of backing index [%s] with [%s] in data stream [%s]",
                                        oldIndex,
                                        newIndex,
                                        dataStreamName
                                    )
                                )
                            );
                        }
                    }, reqListener::onFailure));
            }
        );
    }

    /**
     * Deletes the old backing index and any potential clone after it has been removed from the data stream
     *
     * @param indexName the name of the original backing index
     * @param stepContext the DLM step context
     */
    static void maybeDeleteIndices(String indexName, DlmStepContext stepContext) {
        String cloneIndexName = CloneStep.getDLMCloneIndexName(indexName);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName, cloneIndexName).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);

        stepContext.executeDeduplicatedRequest(
            TransportDeleteIndexAction.TYPE.name(),
            deleteIndexRequest,
            Strings.format("DLM service encountered an error during cleanup for original index [%s]", indexName),
            (req, reqListener) -> {
                logger.debug(
                    "DLM issuing request to delete backing index [{}] and clone index [{}] for cleanup",
                    indexName,
                    cloneIndexName
                );
                stepContext.client()
                    .projectClient(stepContext.projectId())
                    .admin()
                    .indices()
                    .delete(deleteIndexRequest, ActionListener.wrap(resp -> {
                        if (resp.isAcknowledged()) {
                            logger.debug(
                                "DLM Cleanup Step successfully completed cleanup for backing index [{}] & clone index [{}]",
                                indexName,
                                cloneIndexName
                            );
                            reqListener.onResponse(null);
                        } else {
                            reqListener.onFailure(
                                new ElasticsearchException(
                                    Strings.format("DLM Cleanup Step failed to acknowledge delete of backing index [%s]", indexName)
                                )
                            );
                        }
                    }, reqListener::onFailure));
            }
        );
    }
}
