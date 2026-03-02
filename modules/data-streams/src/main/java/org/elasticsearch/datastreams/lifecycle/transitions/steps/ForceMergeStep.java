/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A DLM step responsible for force merging the index.
 */
public class ForceMergeStep implements DlmStep {

    /**
     * Index setting that indicates whether DLM force merge has been completed for this index.
     */
    public static final Setting<Boolean> DLM_FORCE_MERGE_COMPLETE_SETTING = Setting.boolSetting(
        "index.dlm.force_merge_complete",
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private static final Settings FORCE_MERGE_COMPLETE_SETTINGS = Settings.builder()
        .put(DLM_FORCE_MERGE_COMPLETE_SETTING.getKey(), true)
        .build();
    private static final int SINGLE_SEGMENT = 1;
    private static final Logger logger = LogManager.getLogger(ForceMergeStep.class);

    /**
     * Determines if the step has been completed for the given index and project state.
     *
     * @param index        The index to check.
     * @param projectState The current project state.
     * @return True if the step is completed, false otherwise.
     */
    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        return isDLMForceMergeComplete(index, projectState);
    }

    /**
     * This method determines how to execute the step and performs the necessary operations to update the index
     * so that {@link #stepCompleted(Index, ProjectState)} will return true after successful execution.
     *
     * @param stepContext The context and resources for executing the step.
     */
    @Override
    public void execute(DlmStepContext stepContext) {
        maybeForceMerge(stepContext.indexName(), stepContext);
    }

    /**
     * Helper method to check if DLM force merge has been completed for the given index.
     * This reads the {@link #DLM_FORCE_MERGE_COMPLETE_SETTING} from the index metadata.
     *
     * @param index        The index to check.
     * @param projectState The current project state.
     * @return True if the force merge has been completed, false otherwise.
     */
    protected boolean isDLMForceMergeComplete(Index index, ProjectState projectState) {
        return Optional.ofNullable(projectState.metadata().index(index))
            .map(indexMetadata -> DLM_FORCE_MERGE_COMPLETE_SETTING.get(indexMetadata.getSettings()))
            .orElse(false);
    }

    /**
     * Helper method to mark the index as having completed DLM force merge by updating the index setting.
     * This writes the {@link #DLM_FORCE_MERGE_COMPLETE_SETTING} to the index metadata.
     *
     * @param stepContext The context containing the index and client for executing the update.
     * @param listener    The listener to notify upon completion or failure.
     */
    protected void markDLMForceMergeComplete(DlmStepContext stepContext, ActionListener<Void> listener) {
        String indexName = stepContext.indexName();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(FORCE_MERGE_COMPLETE_SETTINGS, indexName);

        String failureMessage = Strings.format(
            "DLM service encountered an error trying to mark force merge as complete for index [%s]",
            indexName
        );

        stepContext.executeDeduplicatedRequest(
            TransportUpdateSettingsAction.TYPE.name(),
            updateSettingsRequest,
            failureMessage,
            (req, unused) -> stepContext.client()
                .projectClient(stepContext.projectId())
                .admin()
                .indices()
                .updateSettings(updateSettingsRequest, ActionListener.wrap(acknowledgedResponse -> {
                    if (acknowledgedResponse.isAcknowledged()) {
                        listener.onResponse(null);
                    } else {
                        listener.onFailure(
                            new ElasticsearchException(
                                Strings.format(
                                    "Failed to mark force merge as complete for index [%s] because "
                                        + "the update settings request was not acknowledged",
                                    indexName
                                )
                            )
                        );
                    }
                }, listener::onFailure))
        );
    }

    /**
     * Helper method to execute the force merge request for the given index. This method forms the request and uses the
     * step context to execute it in a deduplicated manner. The actual execution of the force merge request is
     * delegated to the {@link #forceMerge} method. Checks if the force merge has already been completed for the
     * index before executing and skips execution if so. Also skips if the index does not exist in the project metadata.
     */
    void maybeForceMerge(String index, DlmStepContext stepContext) {
        boolean indexMissing = Optional.ofNullable(stepContext.projectState())
            .map(ProjectState::metadata)
            .map(metadata -> metadata.index(index))
            .isEmpty();

        if (indexMissing) {
            logger.warn("Index [{}] not found in project metadata, skipping force merge step", index);
            return;
        }

        if (isDLMForceMergeComplete(stepContext.index(), stepContext.projectState())) {
            logger.info("DLM force merge step is already completed for index [{}], skipping execution", stepContext.indexName());
            return;
        }

        ForceMergeRequest forceMergeRequest = formForceMergeRequest(index);
        stepContext.executeDeduplicatedRequest(
            ForceMergeAction.NAME,
            forceMergeRequest,
            Strings.format("DLM service encountered an error trying to force merge index [%s]", index),
            (req, l) -> forceMerge(stepContext.projectId(), forceMergeRequest, l, stepContext)
        );
    }

    /**
     * This method executes the given force merge request. Once the request has completed successfully it updates
     * the {@link #DLM_FORCE_MERGE_COMPLETE_SETTING} in the cluster state indicating that the force merge has completed.
     * The listener is notified after the cluster state update has been made, or when the force merge fails or the
     * update to the cluster state fails.
     */
    protected void forceMerge(
        ProjectId projectId,
        ForceMergeRequest forceMergeRequest,
        ActionListener<Void> listener,
        DlmStepContext stepContext
    ) {
        assert forceMergeRequest.indices() != null && forceMergeRequest.indices().length == 1 : "DLM force merges one index at a time";

        final String targetIndex = forceMergeRequest.indices()[0];
        logger.info("DLM is issuing a request to force merge index [{}] to a single segment", targetIndex);
        stepContext.client()
            .projectClient(projectId)
            .admin()
            .indices()
            .forceMerge(forceMergeRequest, listener.delegateFailureAndWrap((l, forceMergeResponse) -> {
                if (forceMergeResponse.getFailedShards() > 0) {
                    DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                    String message = Strings.format(
                        "DLM failed to force merge %d shards for index [%s] due to failures [%s]",
                        forceMergeResponse.getFailedShards(),
                        targetIndex,
                        failures == null
                            ? "unknown"
                            : Arrays.stream(failures).map(DefaultShardOperationFailedException::toString).collect(Collectors.joining(","))
                    );
                    l.onFailure(new ElasticsearchException(message));
                } else if (forceMergeResponse.getTotalShards() != forceMergeResponse.getSuccessfulShards()) {
                    String message = Strings.format(
                        "DLM failed while force merging index [%s]: only %d out of %d shards succeeded",
                        targetIndex,
                        forceMergeResponse.getSuccessfulShards(),
                        forceMergeResponse.getTotalShards()
                    );
                    l.onFailure(new ElasticsearchException(message));
                } else {
                    logger.info("DLM successfully force merged index [{}]", targetIndex);
                    markDLMForceMergeComplete(stepContext, listener);
                }
            }));
    }

    private ForceMergeRequest formForceMergeRequest(String index) {
        ForceMergeRequest req = new ForceMergeRequest(index);
        req.maxNumSegments(SINGLE_SEGMENT);
        return new DataStreamLifecycleService.ForceMergeRequestWrapper(req);
    }

    /**
     * A human-readable name for the step.
     *
     * @return The step name.
     */
    @Override
    public String stepName() {
        return "Force Merge Index";
    }
}
