/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The {@code SnapshotLifecyclePolicyItem} class is a special wrapper almost exactly like the
 * {@link SnapshotLifecyclePolicyMetadata}, however, it elides the headers to ensure that they
 * are not leaked to the user since they may contain sensitive information.
 */
public class SnapshotLifecyclePolicyItem implements ToXContentFragment, Writeable {

    private static final ParseField SNAPSHOT_IN_PROGRESS = new ParseField("in_progress");
    private static final ParseField POLICY_STATS = new ParseField("stats");

    private final SnapshotLifecyclePolicy policy;
    private final long version;
    private final long modifiedDate;
    @Nullable
    private final SnapshotInProgress snapshotInProgress;
    private final SnapshotLifecycleStats.SnapshotPolicyStats policyStats;

    @Nullable
    private final SnapshotInvocationRecord lastSuccess;

    @Nullable
    private final SnapshotInvocationRecord lastFailure;

    public SnapshotLifecyclePolicyItem(
        SnapshotLifecyclePolicyMetadata policyMetadata,
        @Nullable SnapshotInProgress snapshotInProgress,
        @Nullable SnapshotLifecycleStats.SnapshotPolicyStats policyStats
    ) {
        this.policy = policyMetadata.getPolicy();
        this.version = policyMetadata.getVersion();
        this.modifiedDate = policyMetadata.getModifiedDate();
        this.lastSuccess = policyMetadata.getLastSuccess();
        this.lastFailure = policyMetadata.getLastFailure();
        this.snapshotInProgress = snapshotInProgress;
        this.policyStats = policyStats == null ? new SnapshotLifecycleStats.SnapshotPolicyStats(policy.getId()) : policyStats;
    }

    public SnapshotLifecyclePolicyItem(StreamInput in) throws IOException {
        this.policy = new SnapshotLifecyclePolicy(in);
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
        this.lastSuccess = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        this.lastFailure = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        this.snapshotInProgress = in.readOptionalWriteable(SnapshotInProgress::new);
        this.policyStats = new SnapshotLifecycleStats.SnapshotPolicyStats(in);
    }

    // For testing

    SnapshotLifecyclePolicyItem(
        SnapshotLifecyclePolicy policy,
        long version,
        long modifiedDate,
        SnapshotInvocationRecord lastSuccess,
        SnapshotInvocationRecord lastFailure,
        @Nullable SnapshotInProgress snapshotInProgress,
        SnapshotLifecycleStats.SnapshotPolicyStats policyStats
    ) {
        this.policy = policy;
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
        this.snapshotInProgress = snapshotInProgress;
        this.policyStats = policyStats;
    }

    public SnapshotLifecyclePolicy getPolicy() {
        return policy;
    }

    public long getVersion() {
        return version;
    }

    public long getModifiedDate() {
        return modifiedDate;
    }

    public SnapshotInvocationRecord getLastSuccess() {
        return lastSuccess;
    }

    public SnapshotInvocationRecord getLastFailure() {
        return lastFailure;
    }

    @Nullable
    public SnapshotInProgress getSnapshotInProgress() {
        return this.snapshotInProgress;
    }

    public SnapshotLifecycleStats.SnapshotPolicyStats getPolicyStats() {
        return this.policyStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        policy.writeTo(out);
        out.writeVLong(version);
        out.writeVLong(modifiedDate);
        out.writeOptionalWriteable(lastSuccess);
        out.writeOptionalWriteable(lastFailure);
        out.writeOptionalWriteable(snapshotInProgress);
        policyStats.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, version, modifiedDate, lastSuccess, lastFailure, policyStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotLifecyclePolicyItem other = (SnapshotLifecyclePolicyItem) obj;
        return policy.equals(other.policy)
            && version == other.version
            && modifiedDate == other.modifiedDate
            && Objects.equals(lastSuccess, other.lastSuccess)
            && Objects.equals(lastFailure, other.lastFailure)
            && Objects.equals(snapshotInProgress, other.snapshotInProgress)
            && Objects.equals(policyStats, other.policyStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(policy.getId());
        builder.field(SnapshotLifecyclePolicyMetadata.VERSION.getPreferredName(), version);
        builder.timeField(
            SnapshotLifecyclePolicyMetadata.MODIFIED_DATE_MILLIS.getPreferredName(),
            SnapshotLifecyclePolicyMetadata.MODIFIED_DATE.getPreferredName(),
            modifiedDate
        );
        builder.field(SnapshotLifecyclePolicyMetadata.POLICY.getPreferredName(), policy);
        if (lastSuccess != null) {
            builder.field(SnapshotLifecyclePolicyMetadata.LAST_SUCCESS.getPreferredName(), lastSuccess);
        }
        if (lastFailure != null) {
            builder.field(SnapshotLifecyclePolicyMetadata.LAST_FAILURE.getPreferredName(), lastFailure);
        }
        builder.timeField(
            SnapshotLifecyclePolicyMetadata.NEXT_EXECUTION_MILLIS.getPreferredName(),
            SnapshotLifecyclePolicyMetadata.NEXT_EXECUTION.getPreferredName(),
            policy.calculateNextExecution()
        );
        if (snapshotInProgress != null) {
            builder.field(SNAPSHOT_IN_PROGRESS.getPreferredName(), snapshotInProgress);
        }
        builder.startObject(POLICY_STATS.getPreferredName());
        this.policyStats.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class SnapshotInProgress implements ToXContentObject, Writeable {
        private static final ParseField NAME = new ParseField("name");
        private static final ParseField UUID = new ParseField("uuid");
        private static final ParseField STATE = new ParseField("state");
        private static final ParseField START_TIME = new ParseField("start_time_millis");
        private static final ParseField FAILURE = new ParseField("failure");

        private final SnapshotId snapshotId;
        private final SnapshotsInProgress.State state;
        private final long startTime;
        private final String failure;

        public SnapshotInProgress(SnapshotId snapshotId, SnapshotsInProgress.State state, long startTime, @Nullable String failure) {
            this.snapshotId = snapshotId;
            this.state = state;
            this.startTime = startTime;
            this.failure = failure;
        }

        SnapshotInProgress(StreamInput in) throws IOException {
            this.snapshotId = new SnapshotId(in);
            this.state = in.readEnum(SnapshotsInProgress.State.class);
            this.startTime = in.readVLong();
            this.failure = in.readOptionalString();
        }

        public static SnapshotInProgress fromEntry(SnapshotsInProgress.Entry entry) {
            return new SnapshotInProgress(entry.snapshot().getSnapshotId(), entry.state(), entry.startTime(), entry.failure());
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public SnapshotsInProgress.State getState() {
            return state;
        }

        public long getStartTime() {
            return startTime;
        }

        public String getFailure() {
            return failure;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.snapshotId.writeTo(out);
            out.writeEnum(this.state);
            out.writeVLong(this.startTime);
            out.writeOptionalString(this.failure);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME.getPreferredName(), snapshotId.getName());
            builder.field(UUID.getPreferredName(), snapshotId.getUUID());
            builder.field(STATE.getPreferredName(), state);
            builder.timeField(START_TIME.getPreferredName(), "start_time", startTime);
            if (failure != null) {
                builder.field(FAILURE.getPreferredName(), failure);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, state, startTime, failure);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj.getClass() != getClass()) {
                return false;
            }
            SnapshotInProgress other = (SnapshotInProgress) obj;
            return Objects.equals(snapshotId, other.snapshotId)
                && Objects.equals(state, other.state)
                && startTime == other.startTime
                && Objects.equals(failure, other.failure);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
