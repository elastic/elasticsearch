/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.Step;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class SetStepInfoUpdateTask extends IndexLifecycleClusterStateUpdateTask {

    private static final Logger logger = LogManager.getLogger(SetStepInfoUpdateTask.class);

    private final String policy;
    private final ToXContentObject stepInfo;

    public SetStepInfoUpdateTask(Index index, String policy, Step.StepKey currentStepKey, ToXContentObject stepInfo) {
        super(index, currentStepKey);
        this.policy = policy;
        this.stepInfo = stepInfo;
    }

    String getPolicy() {
        return policy;
    }

    ToXContentObject getStepInfo() {
        return stepInfo;
    }

    @Override
    protected ClusterState doExecute(ClusterState currentState) throws IOException {
        final var project = currentState.metadata().getProject();
        IndexMetadata idxMeta = project.index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            return currentState;
        }
        LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
        if (policy.equals(idxMeta.getLifecyclePolicyName()) && Objects.equals(currentStepKey, Step.getCurrentStepKey(lifecycleState))) {
            return ClusterState.builder(currentState)
                .putProjectMetadata(IndexLifecycleTransition.addStepInfoToClusterState(index, project, stepInfo))
                .build();
        } else {
            // either the policy has changed or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void handleFailure(Exception e) {
        logger.warn(
            () -> format("policy [%s] for index [%s] failed trying to set step info for step [%s].", policy, index, currentStepKey),
            e
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetStepInfoUpdateTask that = (SetStepInfoUpdateTask) o;
        return index.equals(that.index)
            && policy.equals(that.policy)
            && currentStepKey.equals(that.currentStepKey)
            && Objects.equals(stepInfo, that.stepInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, policy, currentStepKey, stepInfo);
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
