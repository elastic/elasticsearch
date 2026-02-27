/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;

import java.util.Optional;

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
     * @param dlmStepContext The context and resources for executing the step.
     */
    @Override
    public void execute(DlmStepContext dlmStepContext) {
        // Todo: Implement the force merge logic here.
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
     * A human-readable name for the step.
     *
     * @return The step name.
     */
    @Override
    public String stepName() {
        return "Force Merge Index";
    }
}
