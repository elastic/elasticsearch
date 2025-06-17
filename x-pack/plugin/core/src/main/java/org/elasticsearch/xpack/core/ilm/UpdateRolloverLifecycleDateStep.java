/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

import java.util.function.LongSupplier;

/**
 * Copies the lifecycle reference date to a new index created by rolling over an alias.
 * Used so that the "age" of an index doesn't get reset on rollover.
 */
public class UpdateRolloverLifecycleDateStep extends ClusterStateActionStep {
    private static final Logger logger = LogManager.getLogger(UpdateRolloverLifecycleDateStep.class);
    public static final String NAME = "update-rollover-lifecycle-date";

    private final LongSupplier fallbackTimeSupplier;

    public UpdateRolloverLifecycleDateStep(StepKey key, StepKey nextStepKey, LongSupplier fallbackTimeSupplier) {
        super(key, nextStepKey);
        this.fallbackTimeSupplier = fallbackTimeSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public ProjectState performAction(Index index, ProjectState projectState) {
        IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);

        long newIndexTime;

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetadata.getSettings());
        if (indexingComplete) {
            logger.trace("{} has lifecycle complete set, skipping {}", indexMetadata.getIndex(), UpdateRolloverLifecycleDateStep.NAME);

            // The index won't have RolloverInfo if this is a Following index and indexing_complete was set by CCR,
            // so just use the current time.
            newIndexTime = fallbackTimeSupplier.getAsLong();
        } else {
            final String rolloverTarget = getRolloverTarget(index, projectState.metadata());
            RolloverInfo rolloverInfo = indexMetadata.getRolloverInfos().get(rolloverTarget);
            if (rolloverInfo == null) {
                throw new IllegalStateException(
                    "no rollover info found for ["
                        + indexMetadata.getIndex().getName()
                        + "] with rollover target ["
                        + rolloverTarget
                        + "], the index has not yet rolled over with that target"
                );
            }
            newIndexTime = rolloverInfo.getTime();
        }

        LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState.builder(indexMetadata.getLifecycleExecutionState());
        newLifecycleState.setIndexCreationDate(newIndexTime);
        return projectState.updateProject(projectState.metadata().withLifecycleState(indexMetadata.getIndex(), newLifecycleState.build()));
    }

    private static String getRolloverTarget(Index index, ProjectMetadata project) {
        IndexAbstraction indexAbstraction = project.getIndicesLookup().get(index.getName());
        final String rolloverTarget;
        if (indexAbstraction.getParentDataStream() != null) {
            rolloverTarget = indexAbstraction.getParentDataStream().getName();
        } else {
            // find the newly created index from the rollover and fetch its index.creation_date
            IndexMetadata indexMetadata = project.index(index);
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetadata.getSettings());
            if (Strings.isNullOrEmpty(rolloverAlias)) {
                throw new IllegalStateException(
                    "setting ["
                        + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                        + "] is not set on index ["
                        + indexMetadata.getIndex().getName()
                        + "]"
                );
            }
            rolloverTarget = rolloverAlias;
        }
        return rolloverTarget;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass() && super.equals(obj);
    }
}
