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
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Optional;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;

/**
 * This step clones the index into a new index with 0 replicas.
 */
public class CloneStep implements DlmStep {

    private static final TimeValue CLONE_TIMEOUT = TimeValue.timeValueHours(12);
    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);
    private static final Logger logger = getLogger(CloneStep.class);

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
            markIndexToBeForceMerged(indexName, indexName, stepContext, ActionListener.wrap(resp -> {
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

        cloneIndex(
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

        Long cloneCreationTime = getCloneIndexCreationTime(cloneIndex, cloneIndexMetadata, projectMetadata);

        if (cloneCreationTime == null) {
            throw new IllegalStateException(
                Strings.format("DLM unable to determine creation time for clone index [{}] of index [{}]", cloneIndex, indexName)
            );
        }

        long currentTime = Clock.systemUTC().millis();
        long timeSinceCreation = currentTime - cloneCreationTime;
        if (isCloneIndexStuck(cloneIndexMetadata, timeSinceCreation, stepContext)) {
            // Clone has been stuck for > 12 hours, clean it up so a new clone can be attempted
            TimeValue timeSinceCreationValue = TimeValue.timeValueMillis(timeSinceCreation);
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
            deleteCloneIndexIfExists(
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
            TimeValue timeSinceCreationValue = TimeValue.timeValueMillis(timeSinceCreation);
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

    private static boolean isCloneIndexStuck(IndexMetadata cloneIndexMetadata, long timeSinceCreation, DlmStepContext stepContext) {
        ResizeRequest cloneRequest = formCloneRequest(stepContext.indexName(), cloneIndexMetadata.getIndex().getName());
        return stepContext.isRequestInProgress(cloneRequest) && timeSinceCreation > CLONE_TIMEOUT.millis();
    }

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
            logger.debug("DLM successfully cloned index [{}] to index [{}]", originalIndex, cloneIndex);
            // on success, write the cloned index name to the custom metadata of the index metadata of original index
            markIndexToBeForceMerged(originalIndex, cloneIndex, stepContext, listener.delegateFailure((l, v) -> {
                logger.info("DLM successfully marked index [{}] to be force merged for source index [{}]", cloneIndex, originalIndex);
                l.onResponse(null);
            }));
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> Strings.format("DLM failed to clone index [%s] to index [%s]", originalIndex, cloneIndex), e);
            stepContext.errorStore().recordError(stepContext.projectId(), originalIndex, e);
            deleteCloneIndexIfExists(stepContext, listener.delegateFailure((l, v) -> {
                logger.info(
                    "DLM successfully deleted clone index [{}] after failed attempt to clone index [{}]",
                    cloneIndex,
                    originalIndex
                );
                l.onFailure(e);
            }));
        }
    }

    private void cloneIndex(String originalIndex, String cloneIndex, ActionListener<Void> listener, DlmStepContext stepContext) {
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
    private static void markIndexToBeForceMerged(
        String originalIndex,
        String indexToBeForceMerged,
        DlmStepContext stepContext,
        ActionListener<Void> listener
    ) {
        MarkIndexForDLMForceMergeAction.Request markIndexForForceMergeRequest = new MarkIndexForDLMForceMergeAction.Request(
            stepContext.projectId(),
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

    private static void deleteCloneIndexIfExists(DlmStepContext stepContext, ActionListener<Void> listener) {
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
     * Gets a unique name deterministically for the clone index based on the original index name.
     * The generated name format is "dlm-clone-{hash}-{truncated-original-name}" where the hash is a full 64-character
     * SHA-256 hex digest of the original name to ensure uniqueness, and the original name is truncated if necessary
     * to keep the total name length under 255 bytes.
     *
     * @param originalName the original index name
     * @return a deterministic unique name for the clone index based on the original index name
     */
    public static String getDLMCloneIndexName(String originalName) {
        // Uses full SHA-256 hash (64 hex chars) to ensure uniqueness
        String hash = MessageDigests.toHexString(MessageDigests.sha256().digest(originalName.getBytes(StandardCharsets.UTF_8)));
        String prefix = "dlm-clone-";
        String separator = "-";

        // Calculate max length for original name: 255 - prefix - hash - separator
        int maxOriginalNameLength = 255 - prefix.length() - hash.length() - separator.length();

        // Truncate original name if needed, keeping it in bytes not characters
        String truncatedName = getTruncatedName(originalName, maxOriginalNameLength);

        return prefix + hash + separator + truncatedName;
    }

    private static String getTruncatedName(String originalName, int maxOriginalNameLength) {
        String truncatedName = originalName;
        byte[] nameBytes = originalName.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > maxOriginalNameLength) {
            // Truncate to max length, being careful with multi-byte UTF-8 characters
            truncatedName = new String(nameBytes, 0, maxOriginalNameLength, StandardCharsets.UTF_8);
            // Remove any partial UTF-8 character at the end
            while (truncatedName.getBytes(StandardCharsets.UTF_8).length > maxOriginalNameLength) {
                truncatedName = truncatedName.substring(0, truncatedName.length() - 1);
            }
        }
        return truncatedName;
    }

    /**
     * Gets the creation time of a clone index.
     *
     * @param cloneIndex the name of the clone index
     * @param cloneIndexMetadata the metadata of the clone index
     * @param projectMetadata the project metadata containing data stream information
     * @return the creation time in milliseconds, or null if it cannot be determined
     */
    @Nullable
    protected static Long getCloneIndexCreationTime(String cloneIndex, IndexMetadata cloneIndexMetadata, ProjectMetadata projectMetadata) {
        return Optional.ofNullable(projectMetadata.getIndicesLookup())
            .map(indicesLookup -> indicesLookup.get(cloneIndex))
            .map(IndexAbstraction::getParentDataStream)
            .map(dataStream -> dataStream.getGenerationLifecycleDate(cloneIndexMetadata))
            .map(TimeValue::millis)
            .orElse(cloneIndexMetadata.getCreationDate());
    }

}
