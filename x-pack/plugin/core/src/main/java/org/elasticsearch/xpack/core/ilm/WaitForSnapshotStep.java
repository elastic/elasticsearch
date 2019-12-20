/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Objects;

public class WaitForSnapshotStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-snapshot";

    private static final String MESSAGE_FIELD = "message";
    private static final String POLICY_NOT_EXECUTED_MESSAGE = "waiting for policy '%s' to be executed";
    private static final String POLICY_NOT_FOUND_MESSAGE = "policy '%s' not found, waiting for it to be created and executed";

    private final String policy;

    WaitForSnapshotStep(StepKey key, StepKey nextStepKey, String policy) {
        super(key, nextStepKey);
        this.policy = policy;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        long phaseTime = LifecycleExecutionState.fromIndexMetadata(clusterState.metaData().index(index)).getPhaseTime();

        SnapshotLifecycleMetadata snapMeta = clusterState.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null || snapMeta.getSnapshotConfigurations().containsKey(policy) == false) {
            return new Result(false, info(POLICY_NOT_FOUND_MESSAGE));
        }
        SnapshotLifecyclePolicyMetadata snapPolicyMeta = snapMeta.getSnapshotConfigurations().get(policy);
        if (snapPolicyMeta.getLastSuccess() == null || snapPolicyMeta.getLastSuccess().getTimestamp() < phaseTime) {
            return new Result(false, info(POLICY_NOT_EXECUTED_MESSAGE));
        }

        return new Result(true, null);
    }

    public String getPolicy() {
        return policy;
    }

    private ToXContentObject info(String message) {
        return (builder, params) -> {
            builder.startObject();
            builder.field(MESSAGE_FIELD, String.format(message, policy));
            builder.endObject();
            return builder;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        WaitForSnapshotStep that = (WaitForSnapshotStep) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), policy);
    }
}
