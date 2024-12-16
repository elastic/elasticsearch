/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.step.info.EmptyInfo;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * This {@link Step} waits until the `replicate_for` time of a searchable_snapshot action to pass.
 * <p>
 * It's an {@link AsyncWaitStep} rather than a {@link ClusterStateWaitStep} because we aren't guaranteed to
 * receive a new cluster state in timely fashion when the waiting finishes -- by extending {@link AsyncWaitStep}
 * we are guaranteed to check the condition on each ILM execution.
 */
public class WaitUntilReplicateForTimePassesStep extends AsyncWaitStep {

    public static final String NAME = "check-replicate-for-time-passed";

    private static final Logger logger = LogManager.getLogger(WaitUntilReplicateForTimePassesStep.class);

    private final TimeValue replicateFor;
    private final Supplier<Instant> nowSupplier;

    WaitUntilReplicateForTimePassesStep(StepKey key, StepKey nextStepKey, TimeValue replicateFor, Supplier<Instant> nowSupplier) {
        super(key, nextStepKey, null);
        this.replicateFor = replicateFor;
        this.nowSupplier = nowSupplier;
    }

    WaitUntilReplicateForTimePassesStep(StepKey key, StepKey nextStepKey, TimeValue replicateFor) {
        this(key, nextStepKey, replicateFor, Instant::now);
    }

    public TimeValue getReplicateFor() {
        return this.replicateFor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.replicateFor);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        WaitUntilReplicateForTimePassesStep other = (WaitUntilReplicateForTimePassesStep) obj;
        return super.equals(obj) && Objects.equals(this.replicateFor, other.replicateFor);
    }

    @Override
    public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        IndexMetadata indexMetadata = metadata.index(index);
        assert indexMetadata != null
            : "the index metadata for index [" + index.getName() + "] must exist in the cluster state for step [" + NAME + "]";

        final LifecycleExecutionState executionState = metadata.index(index.getName()).getLifecycleExecutionState();
        assert executionState != null
            : "the lifecycle execution state for index [" + index.getName() + "] must exist in the cluster state for step [" + NAME + "]";

        final Instant endTime = Instant.ofEpochMilli(executionState.phaseTime() + this.replicateFor.millis());
        if (nowSupplier.get().isBefore(endTime)) {
            listener.onResponse(
                false,
                new SingleMessageFieldInfo(
                    Strings.format(
                        "Waiting until the replicate_for time [%s] has elapsed for index [%s] before removing replicas.",
                        this.replicateFor,
                        index.getName()
                    )
                )
            );
            return;
        }

        listener.onResponse(true, EmptyInfo.INSTANCE);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
