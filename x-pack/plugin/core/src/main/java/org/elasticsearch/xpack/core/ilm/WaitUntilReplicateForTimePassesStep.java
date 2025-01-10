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
        final Instant nowTime = nowSupplier.get();
        if (nowTime.isBefore(endTime)) {
            final TimeValue remaining = TimeValue.timeValueMillis(endTime.toEpochMilli() - nowTime.toEpochMilli());
            listener.onResponse(
                false,
                new SingleMessageFieldInfo(
                    Strings.format(
                        "Waiting approximately [%s] until the replicate_for time [%s] has elapsed for index [%s] before removing replicas.",
                        // note: we're sacrificing specificity for stability of string representation. if this string stays the same then
                        // there isn't a cluster state change to update the string (since it is lazy) -- and we'd rather avoid unnecessary
                        // cluster state changes. this approach gives us N days of one cluster state change per day, then ~12 hours of
                        // one cluster state change per hour, and that seems like a reasonable balance between precision and efficiency.
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

    private static final TimeValue TWELVE_HOURS = TimeValue.timeValueHours(12);

    /**
     * Turns a {@link TimeValue} into an approximate time value String. Similar in spirit to {@link TimeValue#toHumanReadableString(int)},
     * but reimplemented here to get slightly different behavior.
     *
     * @param remaining the time remaining
     * @return a String representing the approximate time remaining in days (e.g. "2d") OR hours (e.g. "7h")
     */
    // visible for testing
    static String approximateTimeRemaining(TimeValue remaining) {
        if (remaining.compareTo(TWELVE_HOURS) >= 0) {
            return Math.round(remaining.daysFrac()) + "d";
        } else {
            return Math.round(remaining.hoursFrac()) + "h";
        }
    }

    @Override
    public boolean isRetryable() {
        return true;
    }
}
