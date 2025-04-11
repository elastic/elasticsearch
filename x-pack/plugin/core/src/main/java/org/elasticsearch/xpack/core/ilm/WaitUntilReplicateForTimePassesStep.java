/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

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
        IndexMetadata indexMetadata = metadata.getProject().index(index);
        assert indexMetadata != null
            : "the index metadata for index [" + index.getName() + "] must exist in the cluster state for step [" + NAME + "]";

        final LifecycleExecutionState executionState = metadata.getProject().index(index.getName()).getLifecycleExecutionState();
        assert executionState != null
            : "the lifecycle execution state for index [" + index.getName() + "] must exist in the cluster state for step [" + NAME + "]";

        if (replicateFor == null) {
            // assert at dev-time, but treat this as a no-op at runtime if somehow this should happen (which it shouldn't)
            assert false : "the replicate_for time value for index [" + index.getName() + "] must not be null for step [" + NAME + "]";
            listener.onResponse(true, EmptyInfo.INSTANCE);
            return;
        }

        final Instant endTime = Instant.ofEpochMilli(executionState.phaseTime() + this.replicateFor.millis());
        final Instant nowTime = nowSupplier.get();
        if (nowTime.isBefore(endTime)) {
            final TimeValue remaining = TimeValue.timeValueMillis(endTime.toEpochMilli() - nowTime.toEpochMilli());
            listener.onResponse(
                false,
                new SingleMessageFieldInfo(
                    Strings.format(
                        "Waiting [%s] until the replicate_for time [%s] has elapsed for index [%s] before removing replicas.",
                        // note: we're sacrificing specificity for stability of string representation. if this string stays the same then
                        // there isn't a cluster state change to update the string (since it is lazy) -- and we'd rather avoid unnecessary
                        // cluster state changes. this approach gives us one cluster state change per day, which seems like a reasonable
                        // balance between precision and efficiency.
                        approximateTimeRemaining(remaining),
                        this.replicateFor,
                        index.getName()
                    )
                )
            );
            return;
        }

        listener.onResponse(true, EmptyInfo.INSTANCE);
    }

    private static final TimeValue TWENTY_FOUR_HOURS = TimeValue.timeValueHours(24);

    /**
     * Turns a {@link TimeValue} into a very approximate time value String.
     *
     * @param remaining the time remaining
     * @return a String representing the approximate time remaining in days (e.g. "approximately 2d" OR "less than 1d")
     */
    // visible for testing
    static String approximateTimeRemaining(TimeValue remaining) {
        if (remaining.compareTo(TWENTY_FOUR_HOURS) >= 0) {
            return "approximately " + Math.round(remaining.daysFrac()) + "d";
        } else {
            return "less than 1d";
        }
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
