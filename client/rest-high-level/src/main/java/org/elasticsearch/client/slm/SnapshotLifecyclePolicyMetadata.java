/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Objects;

public class SnapshotLifecyclePolicyMetadata implements ToXContentObject {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField LAST_SUCCESS = new ParseField("last_success");
    static final ParseField LAST_FAILURE = new ParseField("last_failure");
    static final ParseField NEXT_EXECUTION_MILLIS = new ParseField("next_execution_millis");
    static final ParseField NEXT_EXECUTION = new ParseField("next_execution");
    static final ParseField SNAPSHOT_IN_PROGRESS = new ParseField("in_progress");
    static final ParseField POLICY_STATS = new ParseField("stats");

    private final SnapshotLifecyclePolicy policy;
    private final long version;
    private final long modifiedDate;
    private final long nextExecution;
    @Nullable
    private final SnapshotInvocationRecord lastSuccess;
    @Nullable
    private final SnapshotInvocationRecord lastFailure;
    @Nullable
    private final SnapshotInProgress snapshotInProgress;
    private final SnapshotLifecycleStats.SnapshotPolicyStats policyStats;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_metadata",
            a -> {
                SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
                long version = (long) a[1];
                long modifiedDate = (long) a[2];
                SnapshotInvocationRecord lastSuccess = (SnapshotInvocationRecord) a[3];
                SnapshotInvocationRecord lastFailure = (SnapshotInvocationRecord) a[4];
                long nextExecution = (long) a[5];
                SnapshotInProgress sip = (SnapshotInProgress) a[6];
                SnapshotLifecycleStats.SnapshotPolicyStats stats = (SnapshotLifecycleStats.SnapshotPolicyStats) a[7];
                return new SnapshotLifecyclePolicyMetadata(policy, version, modifiedDate, lastSuccess,
                    lastFailure, nextExecution, sip, stats);
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_FAILURE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), NEXT_EXECUTION_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInProgress::parse, SNAPSHOT_IN_PROGRESS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> SnapshotLifecycleStats.SnapshotPolicyStats.parse(p, "policy"), POLICY_STATS);

    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String id) {
        return PARSER.apply(parser, id);
    }

    public SnapshotLifecyclePolicyMetadata(SnapshotLifecyclePolicy policy, long version, long modifiedDate,
                                           SnapshotInvocationRecord lastSuccess, SnapshotInvocationRecord lastFailure,
                                           long nextExecution,
                                           @Nullable SnapshotInProgress snapshotInProgress,
                                           SnapshotLifecycleStats.SnapshotPolicyStats policyStats) {
        this.policy = policy;
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
        this.nextExecution = nextExecution;
        this.snapshotInProgress = snapshotInProgress;
        this.policyStats = policyStats;
    }

    public SnapshotLifecyclePolicy getPolicy() {
        return policy;
    }

    public String getName() {
        return policy.getName();
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

    public long getNextExecution() {
        return this.nextExecution;
    }

    public SnapshotLifecycleStats.SnapshotPolicyStats getPolicyStats() {
        return this.policyStats;
    }

    @Nullable
    public SnapshotInProgress getSnapshotInProgress() {
        return this.snapshotInProgress;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(VERSION.getPreferredName(), version);
        builder.timeField(MODIFIED_DATE_MILLIS.getPreferredName(), MODIFIED_DATE.getPreferredName(), modifiedDate);
        if (Objects.nonNull(lastSuccess)) {
            builder.field(LAST_SUCCESS.getPreferredName(), lastSuccess);
        }
        if (Objects.nonNull(lastFailure)) {
            builder.field(LAST_FAILURE.getPreferredName(), lastFailure);
        }
        builder.timeField(NEXT_EXECUTION_MILLIS.getPreferredName(), NEXT_EXECUTION.getPreferredName(), nextExecution);
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
    public int hashCode() {
        return Objects.hash(policy, version, modifiedDate, lastSuccess, lastFailure, nextExecution, policyStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SnapshotLifecyclePolicyMetadata other = (SnapshotLifecyclePolicyMetadata) obj;
        return Objects.equals(policy, other.policy) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate) &&
            Objects.equals(lastSuccess, other.lastSuccess) &&
            Objects.equals(lastFailure, other.lastFailure) &&
            Objects.equals(nextExecution, other.nextExecution) &&
            Objects.equals(policyStats, other.policyStats);
    }

    public static class SnapshotInProgress implements ToXContentObject {
        private static final ParseField NAME = new ParseField("name");
        private static final ParseField UUID = new ParseField("uuid");
        private static final ParseField STATE = new ParseField("state");
        private static final ParseField START_TIME = new ParseField("start_time_millis");
        private static final ParseField FAILURE = new ParseField("failure");

        private static final ConstructingObjectParser<SnapshotInProgress, Void> PARSER =
            new ConstructingObjectParser<>("snapshot_in_progress", true, a -> {
                SnapshotId id = new SnapshotId((String) a[0], (String) a[1]);
                String state = (String) a[2];
                long start = (long) a[3];
                String failure = (String) a[4];
                return new SnapshotInProgress(id, state, start, failure);
            });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), UUID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), STATE);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), START_TIME);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FAILURE);
        }

        private final SnapshotId snapshotId;
        private final String state;
        private final long startTime;
        private final String failure;

        public SnapshotInProgress(SnapshotId snapshotId, String state, long startTime, @Nullable String failure) {
            this.snapshotId = snapshotId;
            this.state = state;
            this.startTime = startTime;
            this.failure = failure;
        }

        private static SnapshotInProgress parse(XContentParser parser, String name) {
            return PARSER.apply(parser, null);
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
            return Objects.equals(snapshotId, other.snapshotId) &&
                Objects.equals(state, other.state) &&
                startTime == other.startTime &&
                Objects.equals(failure, other.failure);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
