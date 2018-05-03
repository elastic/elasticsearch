/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.function.LongSupplier;

public class MoveToErrorStepUpdateTask extends ClusterStateUpdateTask {
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private LongSupplier nowSupplier;

    public MoveToErrorStepUpdateTask(Index index, String policy, Step.StepKey currentStepKey,
            LongSupplier nowSupplier) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.nowSupplier = nowSupplier;
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

    @Override
    public ClusterState execute(ClusterState currentState) {
        Settings indexSettings = currentState.getMetaData().index(index).getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
            && currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings))) {
            return IndexLifecycleRunner.moveClusterStateToErrorStep(index, currentState, currentStepKey, nowSupplier);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new RuntimeException(e); // NORELEASE implement error handling
    }
}
