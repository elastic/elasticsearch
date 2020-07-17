/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class MoveToErrorStepUpdateTask extends ClusterStateUpdateTask {
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private final BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction;
    private final Consumer<ClusterState> stateChangeConsumer;
    private LongSupplier nowSupplier;
    private Exception cause;

    public MoveToErrorStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey, Exception cause, LongSupplier nowSupplier,
                                     BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction,
                                     Consumer<ClusterState> stateChangeConsumer) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.cause = cause;
        this.nowSupplier = nowSupplier;
        this.stepLookupFunction = stepLookupFunction;
        this.stateChangeConsumer = stateChangeConsumer;
    }

    Index getIndex() {
        return index;
    }

    String getPolicy() {
        return policy;
    }

    Step.StepKey getCurrentStepKey() {
        return currentStepKey;
    }

    Exception getCause() {
        return cause;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws IOException {
        IndexMetadata idxMeta = currentState.getMetadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        Settings indexSettings = idxMeta.getSettings();
        LifecycleExecutionState indexILMData = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(LifecycleExecutionState.getCurrentStepKey(indexILMData))) {
            return IndexLifecycleTransition.moveClusterStateToErrorStep(index, currentState, cause, nowSupplier, stepLookupFunction);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        if (newState.equals(oldState) == false) {
            stateChangeConsumer.accept(newState);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException("policy [" + policy + "] for index [" + index.getName()
                + "] failed trying to move from step [" + currentStepKey + "] to the ERROR step.", e);
    }
}
