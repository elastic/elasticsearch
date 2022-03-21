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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

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
        IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
        if (indexMetadata == null) {
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
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

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
        try {
            if (shouldParseIndexName(indexMetadata.getSettings())) {
                long parsedOriginationDate = parseIndexNameAndExtractDate(index.getName());
                indexMetadataBuilder.settingsVersion(indexMetadata.getSettingsVersion() + 1)
                    .settings(
                        Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(LifecycleSettings.LIFECYCLE_ORIGINATION_DATE, parsedOriginationDate)
                            .build()
                    );
            }
        } catch (Exception e) {
            String policyName = indexMetadata.getLifecyclePolicyName();
            throw new InitializePolicyException(policyName, index.getName(), e);
        }

        // a change may have also been made to the indexMetadata via the indexMetadataBuilder above,
        // so we can't use {@code LifecycleExecutionStateUtils#newClusterStateWithLifecycleState}.
        // instead, we must run through the whole metadata builder and cluster state builder cycle
        indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.asMap());
        Metadata metadata = Metadata.builder(clusterState.metadata()).put(indexMetadataBuilder).build();

        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
