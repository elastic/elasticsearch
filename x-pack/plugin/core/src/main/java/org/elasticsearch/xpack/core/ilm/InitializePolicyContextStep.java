/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.shouldParseIndexName;

/**
 * Initializes the {@link LifecycleExecutionState} for an index. This should be the first Step called on an index.
 */
public final class InitializePolicyContextStep extends ClusterStateActionStep {
    public static final String INITIALIZATION_PHASE = "new";
    public static final StepKey KEY = new StepKey(INITIALIZATION_PHASE, "init", "init");
    private static final Logger logger = LogManager.getLogger(InitializePolicyContextStep.class);

    InitializePolicyContextStep(Step.StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.getMetadata().getProject().index(index);
        if (indexMetadata == null) {
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            // Index must have been since deleted, ignore it
            return clusterState;
        }

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        if (lifecycleState.lifecycleDate() != null) {
            return clusterState;
        }

        LifecycleExecutionState newLifecycleState = LifecycleExecutionState.builder(lifecycleState)
            .setIndexCreationDate(indexMetadata.getCreationDate())
            .build();

        Long parsedOriginationDate = null;
        try {
            if (shouldParseIndexName(indexMetadata.getSettings())) {
                long parsedDate = parseIndexNameAndExtractDate(index.getName()); // can't return null
                parsedOriginationDate = parsedDate;
            }
        } catch (Exception e) {
            String policyName = indexMetadata.getLifecyclePolicyName();
            throw new InitializePolicyException(policyName, index.getName(), e);
        }

        if (parsedOriginationDate == null) {
            // we don't need to update the LifecycleSettings.LIFECYCLE_ORIGINATION_DATE, so we can use the fast path
            return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(
                clusterState,
                indexMetadata.getIndex(),
                newLifecycleState
            );
        } else {
            // we do need to update the LifecycleSettings.LIFECYCLE_ORIGINATION_DATE, so we can't use the fast path
            IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
            builder.settingsVersion(indexMetadata.getSettingsVersion() + 1)
                .settings(
                    Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, parsedOriginationDate)
                        .build()
                );
            builder.putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.asMap());
            return ClusterState.builder(clusterState)
                .putProjectMetadata(ProjectMetadata.builder(clusterState.metadata().getProject()).put(builder).build())
                .build();
        }
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
