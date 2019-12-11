/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;

import java.io.IOException;
import java.util.Objects;

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
        IndexMetaData idxMeta = currentState.getMetaData().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        Settings indexSettings = idxMeta.getSettings();
        LifecycleExecutionState indexILMData = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
                && Objects.equals(currentStepKey, LifecycleExecutionState.getCurrentStepKey(indexILMData))) {
            return IndexLifecycleTransition.addStepInfoToClusterState(index, currentState, stepInfo);
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

    public static class ExceptionWrapper implements ToXContentObject {
        private final Throwable exception;

        public ExceptionWrapper(Throwable exception) {
            this.exception = exception;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, exception);
            builder.endObject();
            return builder;
        }
    }
}
