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
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Date;
import java.util.Locale;
import java.util.Objects;

/***
 * A step that waits for snapshot to be taken by SLM to ensure we have backup before we delete the index.
 * It will signal error if it can't get data needed to do the check (action time from ILM and SLM metadata)
 * and will only return success if execution of SLM policy took place after index entered the wait for snapshot action.
 */
public class WaitForSnapshotStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-snapshot";
    private static final Logger logger = LogManager.getLogger(WaitForSnapshotStep.class);

    private static final String MESSAGE_FIELD = "message";
    private static final String POLICY_NOT_EXECUTED_MESSAGE = "waiting for policy '%s' to be executed since %s";
    private static final String POLICY_NOT_FOUND_MESSAGE = "configured policy '%s' not found";
    private static final String NO_INDEX_METADATA_MESSAGE = "no index metadata found for index '%s'";
    private static final String NO_ACTION_TIME_MESSAGE = "no information about ILM action start in index metadata for index '%s'";

    private final String policy;

    WaitForSnapshotStep(StepKey key, StepKey nextStepKey, String policy) {
        super(key, nextStepKey);
        this.policy = policy;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            throw error(NO_INDEX_METADATA_MESSAGE, index.getName());
        }

        Long actionTime = indexMetadata.getLifecycleExecutionState().actionTime();

        if (actionTime == null) {
            throw error(NO_ACTION_TIME_MESSAGE, index.getName());
        }

        SnapshotLifecycleMetadata snapMeta = clusterState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null || snapMeta.getSnapshotConfigurations().containsKey(policy) == false) {
            throw error(POLICY_NOT_FOUND_MESSAGE, policy);
        }
        SnapshotLifecyclePolicyMetadata snapPolicyMeta = snapMeta.getSnapshotConfigurations().get(policy);
        if (snapPolicyMeta.getLastSuccess() == null
            || snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() == null
            || snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() < actionTime) {
            if (snapPolicyMeta.getLastSuccess() == null) {
                logger.debug("skipping ILM policy execution because there is no last snapshot success, action time: {}", actionTime);
            } else if (snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() == null) {
                /*
                 * This is because we are running in mixed cluster mode, and the snapshot was taken on an older master, which then went
                 * down before this check could happen. We'll wait until a snapshot is taken on this newer master before passing this check.
                 */
                logger.debug("skipping ILM policy execution because no last snapshot start date, action time: {}", actionTime);
            } else {
                logger.debug(
                    "skipping ILM policy execution because snapshot start time {} is before action time {}, snapshot timestamp " + "is {}",
                    snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp(),
                    actionTime,
                    snapPolicyMeta.getLastSuccess().getSnapshotFinishTimestamp()
                );
            }
            return new Result(false, notExecutedMessage(actionTime));
        }
        logger.debug(
            "executing policy because snapshot start time {} is after action time {}, snapshot timestamp is {}",
            snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp(),
            actionTime,
            snapPolicyMeta.getLastSuccess().getSnapshotFinishTimestamp()
        );
        return new Result(true, null);
    }

    public String getPolicy() {
        return policy;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    private ToXContentObject notExecutedMessage(long time) {
        return (builder, params) -> {
            builder.startObject();
            builder.field(MESSAGE_FIELD, String.format(Locale.ROOT, POLICY_NOT_EXECUTED_MESSAGE, policy, new Date(time)));
            builder.endObject();
            return builder;
        };
    }

    private static IllegalStateException error(String message, Object... args) {
        return new IllegalStateException(String.format(Locale.ROOT, message, args));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        WaitForSnapshotStep that = (WaitForSnapshotStep) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), policy);
    }
}
