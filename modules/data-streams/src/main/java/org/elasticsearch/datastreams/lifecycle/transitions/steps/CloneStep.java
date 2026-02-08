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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ProjectState;
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
import java.util.Locale;
import java.util.Optional;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;

/**
 * This step clones the index into a new index with 0 replicas.
 */
public class CloneStep implements DlmStep {

    private static final String DLM_INDEX_TO_BE_MERGED_KEY = "dlm_index_to_be_force_merged";
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
        ProjectId projectId = stepContext.projectId();
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
            markIndexToBeForceMerged(indexName, indexName, stepContext, ActionListener.noop());
            return;
        }
        String cloneIndexName = getCloneIndexName(indexName);
        if (projectMetadata.indices().containsKey(cloneIndexName)) {
            logger.info("DLM cleaning up clone index [{}] for index [{}] as it already exists.", cloneIndexName, indexName);
            deleteCloneIndexIfExists(stepContext);
        }

        ResizeRequest cloneIndexRequest = new ResizeRequest(
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
            ResizeType.CLONE,
            indexName,
            cloneIndexName
        );
        cloneIndexRequest.setTargetIndexSettings(Settings.builder().put("index.number_of_replicas", 0));
        stepContext.executeDeduplicatedRequest(
            cloneIndexRequest,
            Strings.format("DLM service encountered an error when trying to clone index [%s]", indexName),
            (req, reqListener) -> cloneIndex(projectId, cloneIndexRequest, reqListener, stepContext)
        );
    }

    @Override
    public String stepName() {
        return "Clone Index";
    }

    private static class CloneIndexResizeActionListener implements ActionListener<CreateIndexResponse> {
        private final String sourceIndexName;
        private final String targetIndexName;
        private final ActionListener<Void> listener;
        private final DlmStepContext stepContext;

        private CloneIndexResizeActionListener(
            String sourceIndexName,
            String targetIndexName,
            ActionListener<Void> listener,
            DlmStepContext stepContext
        ) {
            this.sourceIndexName = sourceIndexName;
            this.targetIndexName = targetIndexName;
            this.listener = listener;
            this.stepContext = stepContext;
        }

        @Override
        public void onResponse(CreateIndexResponse createIndexResponse) {
            logger.debug("DLM successfully cloned index [{}] to index [{}]", sourceIndexName, targetIndexName);
            // on success, write the cloned index name to the custom metadata of the index metadata of original index
            markIndexToBeForceMerged(sourceIndexName, targetIndexName, stepContext, listener);
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> Strings.format("DLM failed to clone index [%s] to index [%s]", sourceIndexName, targetIndexName), e);
            deleteCloneIndexIfExists(stepContext);
            listener.onFailure(e);
        }
    }

    private void cloneIndex(ProjectId projectId, ResizeRequest cloneRequest, ActionListener<Void> listener, DlmStepContext stepContext) {
        assert cloneRequest.indices() != null && cloneRequest.indices().length == 1 : "DLM should clone one index at a time";
        String sourceIndex = cloneRequest.getSourceIndex();
        String targetIndex = cloneRequest.getTargetIndexRequest().index();
        logger.trace("DLM issuing request to clone index [{}] to index [{}]", sourceIndex, targetIndex);
        CloneIndexResizeActionListener responseListener = new CloneIndexResizeActionListener(
            sourceIndex,
            targetIndex,
            listener,
            stepContext
        );
        stepContext.client().projectClient(projectId).execute(TransportResizeAction.TYPE, cloneRequest, responseListener);
    }

    /*
     * Gets a unique name deterministically for the clone index based on the original index name.
     */
    private static String getCloneIndexName(String originalName) {
        String hash = MessageDigests.toHexString(MessageDigests.sha256().digest(originalName.getBytes(StandardCharsets.UTF_8)))
            .substring(0, 8);
        return "dlm-force-merge-clone-" + originalName + "-" + hash;
    }

    /*
     * Updates the custom metadata of the index metadata of the source index to mark the target index as that to be force merged by DLM.
     * This method performs the update asynchronously using a transport action.
     */
    private static void markIndexToBeForceMerged(
        String sourceIndex,
        String indexToBeForceMerged,
        DlmStepContext stepContext,
        ActionListener<Void> listener
    ) {
        logger.debug("DLM marking index [{}] to be force merged for source index [{}]", indexToBeForceMerged, sourceIndex);
        MarkIndexToBeForceMergedAction.Request request = new MarkIndexToBeForceMergedAction.Request(
            stepContext.projectId(),
            sourceIndex,
            indexToBeForceMerged,
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
        );
        stepContext.client()
            .projectClient(stepContext.projectId())
            .execute(MarkIndexToBeForceMergedAction.INSTANCE, request, listener.delegateFailure((delegate, response) -> {
                logger.debug("DLM successfully marked index [{}] to be force merged", indexToBeForceMerged);
                delegate.onResponse(null);
            }));
    }

    /*
     * Returns the name of index to be force merged by DLM from the custom metadata of the index metadata of the source index.
     * If no such index has been marked in the custom metadata, returns null.
     */
    @Nullable
    private static String getIndexToBeForceMerged(String sourceIndex, ProjectState projectState) {
        return Optional.ofNullable(projectState.metadata().index(sourceIndex))
            .map(indexMetadata -> indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
            .map(customMetadata -> customMetadata.get(DLM_INDEX_TO_BE_MERGED_KEY))
            .orElse(null);
    }

    private static void deleteCloneIndexIfExists(DlmStepContext stepContext) {
        String cloneIndex = getCloneIndexName(stepContext.indexName());
        logger.debug("Attempting to delete index [{}]", cloneIndex);

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(cloneIndex).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        String errorMessage = String.format(Locale.ROOT, "Failed to acknowledge delete of index [%s]", cloneIndex);
        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(
            resp -> logger.debug("DLM successfully deleted clone index [{}]", cloneIndex),
            err -> logger.error(() -> Strings.format("DLM failed to delete clone index [%s]", cloneIndex), err)
        );
        stepContext.client()
            .projectClient(stepContext.projectId())
            .admin()
            .indices()
            .delete(deleteIndexRequest, listener.delegateFailure((delegate, response) -> {
                if (response.isAcknowledged()) {
                    delegate.onResponse(null);
                } else {
                    delegate.onFailure(new ElasticsearchException(errorMessage));
                }
            }));
    }
}
