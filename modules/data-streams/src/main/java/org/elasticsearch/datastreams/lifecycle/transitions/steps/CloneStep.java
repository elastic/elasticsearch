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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Optional;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;

/**
 * This step clones the index into a new index with 0 replicas.
 * The clone index is then marked in the custom metadata of the index metadata
 * of the original index as the index to be force merged by DLM in the next step.
 * If the original index already has 0 replicas, it will be marked directly for force
 * merge without cloning.
 * The step is completed when the clone index (or original index if it had 0 replicas)
 * has all primary shards active, which means it's ready to be force merged in the next step.
 */
public class CloneStep implements DlmStep {

    private static final TimeValue CLONE_TIMEOUT = TimeValue.timeValueHours(12);
    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);
    private static final Logger logger = getLogger(CloneStep.class);
    public static final String CLONE_INDEX_PREFIX = "dlm-clone-";

    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        ProjectMetadata projectMetadata = projectState.metadata();
        // the index can either be "cloned" or the original index if it had 0 replicas
        return Optional.ofNullable(getIndexToBeForceMerged(index.getName(), projectState))
            .map(idx -> projectMetadata.indices().containsKey(idx) ? idx : null)
            .map(idx -> projectState.routingTable().index(idx))
            .map(IndexRoutingTable::allPrimaryShardsActive)
            .orElse(false);
    }

    @Override
    public void execute(DlmStepContext stepContext) {
        Index index = stepContext.index();
        String indexName = index.getName();
        ProjectState projectState = stepContext.projectState();
        ProjectMetadata projectMetadata = projectState.metadata();
        IndexMetadata indexMetadata = projectMetadata.index(index);

        if (indexMetadata == null) {
            logger.warn("Index [{}] not found in project metadata, skipping clone step", indexName);
            return;
        }

        if (indexMetadata.getNumberOfReplicas() == 0) {
            logger.info(
                "Skipping clone step for index [{}] as it already has 0 replicas and can be used for force merge directly",
                indexName
            );
            // mark the index to be force merged directly
            maybeMarkIndexToBeForceMerged(indexName, indexName, stepContext, ActionListener.wrap(resp -> {
                logger.info("DLM successfully marked index [{}] to be force merged", indexName);
            }, err -> {
                logger.error(() -> Strings.format("DLM failed to mark index [%s] to be force merged", indexName), err);
                stepContext.errorStore().recordError(stepContext.projectId(), indexName, err);
            }));
            return;
        }

        String cloneIndex = getDLMCloneIndexName(indexName);
        if (projectMetadata.indices().containsKey(cloneIndex)) {
            // Clone index exists but step not completed - check if it's been stuck for too long and clean up if so
            maybeCleanUpStuckCloneTask(cloneIndex, stepContext);
            return;
        }

        maybeCloneIndex(
            indexName,
            cloneIndex,
            ActionListener.wrap(
                resp -> logger.info("DLM successfully initiated clone of index [{}] to index [{}]", indexName, cloneIndex),
                err -> {
                    logger.error(
                        () -> Strings.format("DLM failed to initiate clone of index [%s] to index [%s]", indexName, cloneIndex),
                        err
                    );
                    stepContext.errorStore().recordError(stepContext.projectId(), indexName, err);
                }
            ),
            stepContext
        );
    }

    @Override
    public String stepName() {
        return "Clone Index";
    }

    /**
     * Checks if the clone index has been stuck for too long and if so, deletes it to allow a new clone attempt.
     */
    private static void maybeCleanUpStuckCloneTask(String cloneIndex, DlmStepContext stepContext) {
        String indexName = stepContext.indexName();
        ProjectMetadata projectMetadata = stepContext.projectState().metadata();
        IndexMetadata cloneIndexMetadata = projectMetadata.index(cloneIndex);
        if (cloneIndexMetadata == null) {
            logger.debug(
                "Clone index [{}] for index [{}] not found in project metadata during stuck clone check, it may have been deleted",
                cloneIndex,
                indexName
            );
            return;
        }

        long cloneCreationTime = getCloneIndexCreationTime(cloneIndex, cloneIndexMetadata, projectMetadata);

        long currentTime = stepContext.clock().millis();
        long timeSinceCreation = currentTime - cloneCreationTime;
        TimeValue timeSinceCreationValue = TimeValue.timeValueMillis(timeSinceCreation);
        if (isCloneIndexStuck(cloneIndexMetadata, timeSinceCreation, stepContext)) {
            // Clone has been stuck for > 12 hours, clean it up so a new clone can be attempted
            logger.info(
                "DLM cleaning up clone index [{}] for index [{}] as it has been in progress for [{}] (raw: [{}ms]), "
                    + "exceeding timeout of [{}] (raw: [{}ms])",
                cloneIndex,
                indexName,
                timeSinceCreationValue.toHumanReadableString(2),
                timeSinceCreation,
                CLONE_TIMEOUT.toHumanReadableString(2),
                CLONE_TIMEOUT.millis()
            );
            maybeDeleteCloneIndex(
                stepContext,
                ActionListener.wrap(
                    resp -> logger.info("DLM successfully cleaned up clone index [{}] for index [{}]", cloneIndex, indexName),
                    err -> {
                        logger.error(
                            () -> Strings.format("DLM failed to clean up clone index [%s] for index [%s]", cloneIndex, indexName),
                            err
                        );
                        stepContext.errorStore().recordError(stepContext.projectId(), indexName, err);
                    }
                )
            );
        } else {
            // Clone is still fresh, wait for it to complete
            logger.debug(
                "DLM clone index [{}] for index [{}] exists and has been in progress for [{}] (raw: [{}ms]), "
                    + "waiting for completion or timeout of [{}] (raw: [{}ms])",
                cloneIndex,
                indexName,
                timeSinceCreationValue.toHumanReadableString(2),
                timeSinceCreation,
                CLONE_TIMEOUT.toHumanReadableString(2),
                CLONE_TIMEOUT.millis()
            );
        }
    }

    /**
     * Determines if a clone index creation is stuck based on whether the clone request is still
     * in progress or the clone shards are not active, and if the defined timeout has been breached.
     */
    private static boolean isCloneIndexStuck(IndexMetadata cloneIndexMetadata, long timeSinceCreation, DlmStepContext stepContext) {
        ResizeRequest cloneRequest = formCloneRequest(stepContext.indexName(), cloneIndexMetadata.getIndex().getName());
        boolean cloneShardsAreActive = Optional.ofNullable(cloneIndexMetadata.getIndex())
            .map(Index::getName)
            .map(stepContext.projectState().routingTable()::index)
            .map(IndexRoutingTable::allPrimaryShardsActive)
            .orElse(false);
        return (stepContext.isRequestInProgress(cloneRequest) || cloneShardsAreActive == false)
            && timeSinceCreation > CLONE_TIMEOUT.millis();
    }

    /**
     * Listener for the clone index action that will mark the cloned index to be force merged on success within the index metadata of the
     * original index, or clean up the clone index on failure.
     */
    private static class CloneIndexResizeActionListener implements ActionListener<CreateIndexResponse> {
        private final String originalIndex;
        private final String cloneIndex;
        private final ActionListener<Void> listener;
        private final DlmStepContext stepContext;

        private CloneIndexResizeActionListener(
            String originalIndex,
            String cloneIndex,
            ActionListener<Void> listener,
            DlmStepContext stepContext
        ) {
            this.originalIndex = originalIndex;
            this.cloneIndex = cloneIndex;
            this.listener = listener;
            this.stepContext = stepContext;
        }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            if (createIndexResponse.isAcknowledged() == false) {
                onFailure(
                    new ElasticsearchException(
                        Strings.format("DLM failed to acknowledge clone of index [%s] to index [%s]", originalIndex, cloneIndex)
                    )
                );
                return;
            }
            logger.info("DLM successfully cloned index [{}] to index [{}]", originalIndex, cloneIndex);
            // on success, write the cloned index name to the custom metadata of the index metadata of original index
            maybeMarkIndexToBeForceMerged(originalIndex, cloneIndex, stepContext, listener.delegateFailure((l, v) -> {
                logger.info("DLM successfully marked index [{}] to be force merged for source index [{}]", cloneIndex, originalIndex);
                l.onResponse(null);
            }));
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> Strings.format("DLM failed to clone index [%s] to index [%s]", originalIndex, cloneIndex), e);
            stepContext.errorStore().recordError(stepContext.projectId(), originalIndex, e);
            maybeDeleteCloneIndex(stepContext, listener.delegateFailure((l, v) -> {
                logger.info(
                    "DLM successfully deleted clone index [{}] after failed attempt to clone index [{}]",
                    cloneIndex,
                    originalIndex
                );
                l.onFailure(e);
            }));
        }
    }

    private void maybeCloneIndex(String originalIndex, String cloneIndex, ActionListener<Void> listener, DlmStepContext stepContext) {
        ResizeRequest cloneIndexRequest = formCloneRequest(originalIndex, cloneIndex);
        stepContext.executeDeduplicatedRequest(
            TransportResizeAction.TYPE.name(),
            cloneIndexRequest,
            Strings.format("DLM service encountered an error when trying to clone index [%s]", originalIndex),
            (req, unused) -> cloneIndex(stepContext.projectId(), cloneIndexRequest, listener, stepContext)
        );
    }

    private void cloneIndex(ProjectId projectId, ResizeRequest cloneRequest, ActionListener<Void> listener, DlmStepContext stepContext) {
        assert cloneRequest.indices() != null && cloneRequest.indices().length == 1 : "DLM should clone one index at a time";
        String originalIndex = cloneRequest.getSourceIndex();
        String cloneIndex = cloneRequest.getTargetIndexRequest().index();
        logger.trace("DLM issuing request to clone index [{}] to index [{}]", originalIndex, cloneIndex);
        CloneIndexResizeActionListener responseListener = new CloneIndexResizeActionListener(
            originalIndex,
            cloneIndex,
            listener,
            stepContext
        );
        stepContext.client().projectClient(projectId).execute(TransportResizeAction.TYPE, cloneRequest, responseListener);
    }

    /**
     * Updates the custom metadata of the index metadata of the source index to mark the target index as that to be force merged by DLM.
     * This method performs the update asynchronously using a transport action.
     */
    private static void maybeMarkIndexToBeForceMerged(
        String originalIndex,
        String indexToBeForceMerged,
        DlmStepContext stepContext,
        ActionListener<Void> listener
    ) {
        MarkIndexForDLMForceMergeAction.Request markIndexForForceMergeRequest = new MarkIndexForDLMForceMergeAction.Request(
            originalIndex,
            indexToBeForceMerged
        );
        stepContext.executeDeduplicatedRequest(
            MarkIndexForDLMForceMergeAction.TYPE.name(),
            markIndexForForceMergeRequest,
            Strings.format(
                "DLM service encountered an error when trying to mark index [%s] to be force merged for source index [%s]",
                indexToBeForceMerged,
                originalIndex
            ),
            (req, unused) -> markIndexToBeForceMerged(markIndexForForceMergeRequest, stepContext, listener)
        );
    }

    private static void markIndexToBeForceMerged(
        MarkIndexForDLMForceMergeAction.Request request,
        DlmStepContext stepContext,
        ActionListener<Void> listener
    ) {
        logger.debug(
            "DLM marking index [{}] to be force merged for source index [{}]",
            request.getIndexToBeForceMerged(),
            request.getOriginalIndex()
        );
        stepContext.client()
            .projectClient(stepContext.projectId())
            .execute(MarkIndexForDLMForceMergeAction.TYPE, request, ActionListener.wrap(resp -> {
                if (resp.isAcknowledged()) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(
                        new ElasticsearchException(
                            Strings.format(
                                "DLM failed to acknowledge marking index [%s] to be force merged for source index [%s]",
                                request.getIndexToBeForceMerged(),
                                request.getOriginalIndex()
                            )
                        )
                    );
                }
            }, listener::onFailure));
    }

    private static void maybeDeleteCloneIndex(DlmStepContext stepContext, ActionListener<Void> listener) {
        String cloneIndex = getDLMCloneIndexName(stepContext.indexName());
        if (stepContext.projectState().metadata().indices().containsKey(cloneIndex) == false) {
            logger.debug("Clone index [{}] does not exist, no need to delete", cloneIndex);
            listener.onResponse(null);
            return;
        }
        logger.debug("Attempting to delete index [{}]", cloneIndex);

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(cloneIndex).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        stepContext.executeDeduplicatedRequest(
            TransportDeleteIndexAction.TYPE.name(),
            deleteIndexRequest,
            Strings.format("DLM service encountered an error trying to delete clone index [%s]", cloneIndex),
            (req, unused) -> deleteCloneIndex(deleteIndexRequest, stepContext, listener)
        );
    }

    private static void deleteCloneIndex(DeleteIndexRequest deleteIndexRequest, DlmStepContext stepContext, ActionListener<Void> listener) {
        String cloneIndex = deleteIndexRequest.indices()[0];
        logger.debug("DLM issuing request to delete index [{}]", cloneIndex);
        stepContext.client()
            .projectClient(stepContext.projectId())
            .admin()
            .indices()
            .delete(deleteIndexRequest, ActionListener.wrap(resp -> {
                if (resp.isAcknowledged()) {
                    logger.debug("DLM successfully deleted clone index [{}]", cloneIndex);
                    listener.onResponse(null);
                } else {
                    listener.onFailure(
                        new ElasticsearchException(Strings.format("Failed to acknowledge delete of index [%s]", cloneIndex))
                    );
                }
            }, err -> {
                if (err instanceof IndexNotFoundException) {
                    // If the index was not found, it means it was already deleted, so we can consider this a success
                    logger.debug("Clone index [{}] was not found during DLM delete attempt, it may have already been deleted", cloneIndex);
                    listener.onResponse(null);
                    return;
                }
                logger.error(() -> Strings.format("DLM failed to delete clone index [%s]", cloneIndex), err);
                listener.onFailure(err);
            }));
    }

    /**
     * Forms a resize request to clone the source index into a new index with 0 replicas.
     * @param originalIndex the index to be cloned
     * @param cloneIndex the name of the new index to clone into
     * @return the resize request to clone the source index into a new index with 0 replicas
     */
    public static ResizeRequest formCloneRequest(String originalIndex, String cloneIndex) {
        CreateIndexRequest createReq = new CreateIndexRequest(cloneIndex);
        createReq.waitForActiveShards(ActiveShardCount.ALL);
        ResizeRequest resizeReq = new ResizeRequest(
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
            ResizeType.CLONE,
            originalIndex,
            cloneIndex
        );
        resizeReq.setTargetIndex(createReq);
        resizeReq.setTargetIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        return resizeReq;
    }

    /**
     * Returns the name of index to be force merged by DLM from the custom metadata of the index metadata of the source index.
     * If no such index has been marked in the custom metadata, returns null.
     */
    @Nullable
    private static String getIndexToBeForceMerged(String originalIndex, ProjectState projectState) {
        return Optional.ofNullable(projectState.metadata().index(originalIndex))
            .map(indexMetadata -> indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
            .map(customMetadata -> customMetadata.get(DLM_INDEX_FOR_FORCE_MERGE_KEY))
            .orElse(null);
    }

    /**
     * Gets a prefixed name for the clone index based on the original index name
     *
     * @param originalName the original index name
     * @return a prefixed clone index name
     */
    public static String getDLMCloneIndexName(String originalName) {
        return CLONE_INDEX_PREFIX + originalName;
    }

    /**
     * Gets the creation time of the clone index in milliseconds. If the clone index is part of a data stream, it attempts
     * to get the creation time from the data stream's generation lifecycle date for the clone index. If that is not available,
     * it falls back to the creation date of the clone index metadata.
     *
     * @param cloneIndex the name of the clone index
     * @param cloneIndexMetadata the metadata of the clone index
     * @param projectMetadata the project metadata containing data stream information
     * @return the creation time in milliseconds
     */
    protected static long getCloneIndexCreationTime(String cloneIndex, IndexMetadata cloneIndexMetadata, ProjectMetadata projectMetadata) {
        return Optional.ofNullable(projectMetadata.getIndicesLookup())
            .map(indicesLookup -> indicesLookup.get(cloneIndex))
            .map(IndexAbstraction::getParentDataStream)
            .map(dataStream -> dataStream.getGenerationLifecycleDate(cloneIndexMetadata))
            .map(TimeValue::millis)
            .orElse(cloneIndexMetadata.getCreationDate());
    }

}
