/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * Copies the execution state data from one index to another, typically after a
 * new index has been created. As part of the execution state copy it will set the target index
 * "current step" to the provided target next step {@link org.elasticsearch.xpack.core.ilm.Step.StepKey}.
 * <p>
 * Useful for actions such as shrink.
 */
public class CopyExecutionStateStep extends ClusterStateActionStep {
    public static final String NAME = "copy-execution-state";

    private static final Logger logger = LogManager.getLogger(CopyExecutionStateStep.class);

    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;
    private final StepKey targetNextStepKey;
    private final SetOnce<String> calculatedTargetIndexName = new SetOnce<>();

    public CopyExecutionStateStep(
        StepKey key,
        StepKey nextStepKey,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier,
        StepKey targetNextStepKey
    ) {
        super(key, nextStepKey);
        this.targetIndexNameSupplier = targetIndexNameSupplier;
        this.targetNextStepKey = targetNextStepKey;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    BiFunction<String, LifecycleExecutionState, String> getTargetIndexNameSupplier() {
        return targetIndexNameSupplier;
    }

    StepKey getTargetNextStepKey() {
        return targetNextStepKey;
    }

    @Override
    public Tuple<String, StepKey> indexForAsyncInvocation() {
        assert calculatedTargetIndexName.get() != null : "attempted to retrieve the index for async invocation before it was set";
        return new Tuple<>(calculatedTargetIndexName.get(), targetNextStepKey);
    }

    @Override
    public ProjectState performAction(Index index, ProjectState projectState) {
        IndexMetadata indexMetadata = projectState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            return projectState;
        }
        // get target index
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        String targetIndexName = targetIndexNameSupplier.apply(index.getName(), lifecycleState);
        calculatedTargetIndexName.set(targetIndexName);
        IndexMetadata targetIndexMetadata = projectState.metadata().index(targetIndexName);

        if (targetIndexMetadata == null) {
            logger.warn(
                "[{}] index [{}] unable to copy execution state to target index [{}] as target index does not exist",
                getKey().action(),
                index.getName(),
                targetIndexName
            );
            throw new IllegalStateException(
                "unable to copy execution state from [" + index.getName() + "] to [" + targetIndexName + "] as target index does not exist"
            );
        }

        String phase = targetNextStepKey.phase();
        String action = targetNextStepKey.action();
        String step = targetNextStepKey.name();

        LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState.builder(lifecycleState);
        // Override the phase, action, and step for the target next StepKey
        newLifecycleState.setPhase(phase);
        newLifecycleState.setAction(action);
        newLifecycleState.setStep(step);

        // Build a new index metadata with the version incremented and the new lifecycle state.
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(targetIndexMetadata)
            .version(targetIndexMetadata.getVersion() + 1)
            .putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.build().asMap());

        // Remove the skip setting if it's present.
        if (targetIndexMetadata.getSettings().hasValue(LifecycleSettings.LIFECYCLE_SKIP)) {
            final var newSettings = Settings.builder().put(targetIndexMetadata.getSettings());
            newSettings.remove(LifecycleSettings.LIFECYCLE_SKIP);
            indexMetadataBuilder.settingsVersion(targetIndexMetadata.getSettingsVersion() + 1).settings(newSettings);
        }

        return projectState.updateProject(
            ProjectMetadata.builder(projectState.metadata()).put(indexMetadataBuilder.build(), false).build()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CopyExecutionStateStep that = (CopyExecutionStateStep) o;
        return super.equals(o)
            && Objects.equals(targetIndexNameSupplier, that.targetIndexNameSupplier)
            && Objects.equals(targetNextStepKey, that.targetNextStepKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexNameSupplier, targetNextStepKey);
    }
}
