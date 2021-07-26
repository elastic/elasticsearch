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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Date;
import java.util.Locale;
import java.util.Objects;

/***
 * A step that waits for snapshot to be taken by SLM to ensure we have backup before we delete the index.
 * It will signal error if it can't get data needed to do the check (phase time from ILM and SLM metadata)
 * and will only return success if execution of SLM policy took place after index entered deleted phase.
 */
public class WaitForSnapshotStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-snapshot";
    private static final Logger logger = LogManager.getLogger(WaitForSnapshotStep.class);

    private static final String MESSAGE_FIELD = "message";
    private static final String POLICY_NOT_EXECUTED_MESSAGE = "waiting for policy '%s' to be executed since %s";
    private static final String POLICY_NOT_FOUND_MESSAGE = "configured policy '%s' not found";
    private static final String NO_INDEX_METADATA_MESSAGE = "no index metadata found for index '%s'";
    private static final String NO_PHASE_TIME_MESSAGE = "no information about ILM phase start in index metadata for index '%s'";

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

        Long phaseTime = LifecycleExecutionState.fromIndexMetadata(indexMetadata).getPhaseTime();

        if (phaseTime == null) {
            throw error(NO_PHASE_TIME_MESSAGE, index.getName());
        }

        SnapshotLifecycleMetadata snapMeta = clusterState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null || snapMeta.getSnapshotConfigurations().containsKey(policy) == false) {
            throw error(POLICY_NOT_FOUND_MESSAGE, policy);
        }
        SnapshotLifecyclePolicyMetadata snapPolicyMeta = snapMeta.getSnapshotConfigurations().get(policy);
        if (snapPolicyMeta.getLastSuccess() == null || snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() == null ||
            snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() < phaseTime) {
            //TODO: Clean up all this logging once I figure out the TimeSeriesLiefecycleActionsIT timing problems
            if (snapPolicyMeta.getLastSuccess() == null) {
                logger.info("Not executing policy because no last snapshot success. Phase time: {}", phaseTime);
            } else if (snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp() == null) {
                logger.info("Not executing policy because no last snapshot start date.");
            }
            else {
                logger.info("Not executing policy because snapshot start time {} is before phase time {}. Snapshot timestamp is {}",
                    snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp(), phaseTime, snapPolicyMeta.getLastSuccess().getSnapshotFinishTimestamp());
            }
            return new Result(false, notExecutedMessage(phaseTime));
        }
        logger.info("Executing policy because snapshot start time {} is after phase time {}. Snapshot timestamp is {}",
            snapPolicyMeta.getLastSuccess().getSnapshotStartTimestamp(), phaseTime, snapPolicyMeta.getLastSuccess().getSnapshotFinishTimestamp());
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

    private IllegalStateException error(String message, Object... args) {
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
