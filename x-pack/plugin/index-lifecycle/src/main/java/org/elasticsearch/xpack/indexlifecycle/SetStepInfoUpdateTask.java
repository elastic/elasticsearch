/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.io.IOException;

public class SetStepInfoUpdateTask extends ClusterStateUpdateTask {
    private final Index index;
    private final String policy;
    private final Step.StepKey currentStepKey;
    private ToXContentObject stepInfo;

    public SetStepInfoUpdateTask(Index index, String policy, Step.StepKey currentStepKey, ToXContentObject stepInfo) {
        this.index = index;
        this.policy = policy;
        this.currentStepKey = currentStepKey;
        this.stepInfo = stepInfo;
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

    ToXContentObject getStepInfo() {
        return stepInfo;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws IOException {
        Settings indexSettings = currentState.getMetaData().index(index).getSettings();
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
                && currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings))) {
            return IndexLifecycleRunner.addStepInfoToClusterState(index, currentState, stepInfo);
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException("policy [" + policy + "] for index [" + index.getName()
                + "] failed trying to set step info for step [" + currentStepKey + "].", e);
    }
}
