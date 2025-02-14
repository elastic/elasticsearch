/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SnapshotLifecycleStats contains metrics and stats about snapshot lifecycle policy execution - how
 * many snapshots were taken, deleted, how many failures, etc. It contains both global stats
 * (snapshots taken, retention runs), and per-policy stats.
 */
public class SnapshotLifecycleStats implements Writeable, ToXContentObject {

    private final long retentionRun;
    private final long retentionFailed;
    private final long retentionTimedOut;
    private final long retentionTimeMs;
    private final Map<String, SnapshotPolicyStats> policyStats;
    public static final ParseField RETENTION_RUNS = new ParseField("retention_runs");
    public static final ParseField RETENTION_FAILED = new ParseField("retention_failed");
    public static final ParseField RETENTION_TIMED_OUT = new ParseField("retention_timed_out");
    public static final ParseField RETENTION_TIME = new ParseField("retention_deletion_time");
    public static final ParseField RETENTION_TIME_MILLIS = new ParseField("retention_deletion_time_millis");
    public static final ParseField POLICY_STATS = new ParseField("policy_stats");
    public static final ParseField TOTAL_TAKEN = new ParseField("total_snapshots_taken");
    public static final ParseField TOTAL_FAILED = new ParseField("total_snapshots_failed");
    public static final ParseField TOTAL_DELETIONS = new ParseField("total_snapshots_deleted");
    public static final ParseField TOTAL_DELETION_FAILURES = new ParseField("total_snapshot_deletion_failures");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotLifecycleStats, Void> PARSER = new ConstructingObjectParser<>(
        "snapshot_policy_stats",
        true,
        a -> {
            long runs = (long) a[0];
            long failed = (long) a[1];
            long timedOut = (long) a[2];
            long timeMs = (long) a[3];
            Map<String, SnapshotPolicyStats> policyStatsMap = ((List<SnapshotPolicyStats>) a[4]).stream()
                .collect(Collectors.toMap(m -> m.policyId, Function.identity()));
            return new SnapshotLifecycleStats(runs, failed, timedOut, timeMs, Collections.unmodifiableMap(policyStatsMap));
        }
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_RUNS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_FAILED);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_TIMED_OUT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_TIME_MILLIS);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), SnapshotPolicyStats.PARSER, POLICY_STATS);
    }

    public SnapshotLifecycleStats() {
        this(0, 0, 0, 0, Map.of());
    }

    // public for testing
    public SnapshotLifecycleStats(
        long retentionRuns,
        long retentionFailed,
        long retentionTimedOut,
        long retentionTimeMs,
        Map<String, SnapshotPolicyStats> policyStats
    ) {
        this.retentionRun = retentionRuns;
        this.retentionFailed = retentionFailed;
        this.retentionTimedOut = retentionTimedOut;
        this.retentionTimeMs = retentionTimeMs;
        this.policyStats = Collections.unmodifiableMap(policyStats);
    }

    private SnapshotLifecycleStats(Map<String, SnapshotPolicyStats> policyStats) {
        this(0, 0, 0, 0, policyStats);
    }

    public SnapshotLifecycleStats(StreamInput in) throws IOException {
        this.policyStats = in.readImmutableMap(SnapshotPolicyStats::new);
        this.retentionRun = in.readVLong();
        this.retentionFailed = in.readVLong();
        this.retentionTimedOut = in.readVLong();
        this.retentionTimeMs = in.readVLong();
    }

    public static SnapshotLifecycleStats parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SnapshotLifecycleStats merge(SnapshotLifecycleStats other) {
        HashMap<String, SnapshotPolicyStats> newPolicyStats = new HashMap<>(this.policyStats);
        // Merges the per-run stats (the stats in "other") with the stats already present
        other.policyStats.forEach((policyId, perRunPolicyStats) -> {
            newPolicyStats.compute(policyId, (k, existingPolicyMetrics) -> {
                if (existingPolicyMetrics == null) {
                    return perRunPolicyStats;
                } else {
                    return existingPolicyMetrics.merge(perRunPolicyStats);
                }
            });
        });

        return new SnapshotLifecycleStats(
            this.retentionRun + other.retentionRun,
            this.retentionFailed + other.retentionFailed,
            this.retentionTimedOut + other.retentionTimedOut,
            this.retentionTimeMs + other.retentionTimeMs,
            Collections.unmodifiableMap(newPolicyStats)
        );
    }

    public SnapshotLifecycleStats removePolicy(String policyId) {
        Map<String, SnapshotPolicyStats> policyStatsCopy = new HashMap<>(this.policyStats);
        policyStatsCopy.remove(policyId);
        return new SnapshotLifecycleStats(
            this.retentionRun,
            this.retentionFailed,
            this.retentionTimedOut,
            this.retentionTimeMs,
            Collections.unmodifiableMap(policyStatsCopy)
        );
    }

    /**
     * @return a map of per-policy stats for each SLM policy
     */
    public Map<String, SnapshotPolicyStats> getMetrics() {
        return this.policyStats;
    }

    /**
     * Return new stats with number of times SLM retention has been run incremented
     */
    public SnapshotLifecycleStats withRetentionRunIncremented() {
        return new SnapshotLifecycleStats(retentionRun + 1, retentionFailed, retentionTimedOut, retentionTimeMs, policyStats);
    }

    /**
     * Return new stats with number of times SLM retention has failed incremented
     */
    public SnapshotLifecycleStats withRetentionFailedIncremented() {
        return new SnapshotLifecycleStats(retentionRun, retentionFailed + 1, retentionTimedOut, retentionTimeMs, policyStats);
    }

    /**
     * Return new stats the number of times that SLM retention timed out due to the max delete time
     * window being exceeded incremented
     */
    public SnapshotLifecycleStats withRetentionTimedOutIncremented() {
        return new SnapshotLifecycleStats(retentionRun, retentionFailed, retentionTimedOut + 1, retentionTimeMs, policyStats);
    }

    /**
     * Return new stats with the amount of time taken for deleting snapshots during SLM retention updated
     */
    public SnapshotLifecycleStats withDeletionTimeUpdated(TimeValue elapsedTime) {
        final long newRetentionTimeMs = retentionTimeMs + elapsedTime.millis();
        return new SnapshotLifecycleStats(retentionRun, retentionFailed, retentionTimedOut, newRetentionTimeMs, policyStats);
    }

    /**
     * Return new stats with the per-policy snapshot taken count for the given policy id incremented
     */
    public SnapshotLifecycleStats withTakenIncremented(String slmPolicy) {
        return merge(new SnapshotLifecycleStats(Map.of(slmPolicy, SnapshotPolicyStats.taken(slmPolicy))));
    }

    /**
     * Return new stats with the per-policy snapshot failure count for the given policy id incremented
     */
    public SnapshotLifecycleStats withFailedIncremented(String slmPolicy) {
        return merge(new SnapshotLifecycleStats(Map.of(slmPolicy, SnapshotPolicyStats.failed(slmPolicy))));
    }

    /**
     * Return new stats with the per-policy snapshot deleted count for the given policy id incremented
     */
    public SnapshotLifecycleStats withDeletedIncremented(String slmPolicy) {
        return merge(new SnapshotLifecycleStats(Map.of(slmPolicy, SnapshotPolicyStats.deleted(slmPolicy))));
    }

    /**
     * Return new stats with the per-policy snapshot deletion failure count for the given policy id incremented
     */
    public SnapshotLifecycleStats withDeleteFailureIncremented(String slmPolicy) {
        return merge(new SnapshotLifecycleStats(Map.of(slmPolicy, SnapshotPolicyStats.deleteFailure(slmPolicy))));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(policyStats, StreamOutput::writeWriteable);
        out.writeVLong(retentionRun);
        out.writeVLong(retentionFailed);
        out.writeVLong(retentionTimedOut);
        out.writeVLong(retentionTimeMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RETENTION_RUNS.getPreferredName(), this.retentionRun);
        builder.field(RETENTION_FAILED.getPreferredName(), this.retentionFailed);
        builder.field(RETENTION_TIMED_OUT.getPreferredName(), this.retentionTimedOut);
        TimeValue retentionTime = TimeValue.timeValueMillis(this.retentionTimeMs);
        builder.field(RETENTION_TIME.getPreferredName(), retentionTime);
        builder.field(RETENTION_TIME_MILLIS.getPreferredName(), retentionTime.millis());

        List<SnapshotPolicyStats> metrics = getMetrics().values()
            .stream()
            .sorted(Comparator.comparing(SnapshotPolicyStats::getPolicyId)) // maintain a consistent order when serializing
            .toList();
        long totalTaken = metrics.stream().mapToLong(s -> s.snapshotsTaken).sum();
        long totalFailed = metrics.stream().mapToLong(s -> s.snapshotsFailed).sum();
        long totalDeleted = metrics.stream().mapToLong(s -> s.snapshotsDeleted).sum();
        long totalDeleteFailures = metrics.stream().mapToLong(s -> s.snapshotDeleteFailures).sum();
        builder.field(TOTAL_TAKEN.getPreferredName(), totalTaken);
        builder.field(TOTAL_FAILED.getPreferredName(), totalFailed);
        builder.field(TOTAL_DELETIONS.getPreferredName(), totalDeleted);
        builder.field(TOTAL_DELETION_FAILURES.getPreferredName(), totalDeleteFailures);

        builder.startArray(POLICY_STATS.getPreferredName());
        for (SnapshotPolicyStats stats : metrics) {
            builder.startObject();
            stats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(retentionRun, retentionFailed, retentionTimedOut, retentionTimeMs, policyStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotLifecycleStats other = (SnapshotLifecycleStats) obj;
        return Objects.equals(retentionRun, other.retentionRun)
            && Objects.equals(retentionFailed, other.retentionFailed)
            && Objects.equals(retentionTimedOut, other.retentionTimedOut)
            && Objects.equals(retentionTimeMs, other.retentionTimeMs)
            && Objects.equals(policyStats, other.policyStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class SnapshotPolicyStats implements Writeable, ToXContentFragment {
        private final String policyId;
        private final long snapshotsTaken;
        private final long snapshotsFailed;
        private final long snapshotsDeleted;
        private final long snapshotDeleteFailures;

        public static final ParseField POLICY_ID = new ParseField("policy");
        public static final ParseField SNAPSHOTS_TAKEN = new ParseField("snapshots_taken");
        public static final ParseField SNAPSHOTS_FAILED = new ParseField("snapshots_failed");
        public static final ParseField SNAPSHOTS_DELETED = new ParseField("snapshots_deleted");
        public static final ParseField SNAPSHOT_DELETION_FAILURES = new ParseField("snapshot_deletion_failures");

        static final ConstructingObjectParser<SnapshotPolicyStats, Void> PARSER = new ConstructingObjectParser<>(
            "snapshot_policy_stats",
            true,
            a -> {
                String id = (String) a[0];
                long taken = (long) a[1];
                long failed = (long) a[2];
                long deleted = (long) a[3];
                long deleteFailed = (long) a[4];
                return new SnapshotPolicyStats(id, taken, failed, deleted, deleteFailed);
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_TAKEN);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_FAILED);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_DELETED);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOT_DELETION_FAILURES);
        }

        public SnapshotPolicyStats(String slmPolicy) {
            this(slmPolicy, 0, 0, 0, 0);
        }

        public SnapshotPolicyStats(String policyId, long snapshotsTaken, long snapshotsFailed, long deleted, long failedDeletes) {
            this.policyId = policyId;
            this.snapshotsTaken = snapshotsTaken;
            this.snapshotsFailed = snapshotsFailed;
            this.snapshotsDeleted = deleted;
            this.snapshotDeleteFailures = failedDeletes;
        }

        public SnapshotPolicyStats(StreamInput in) throws IOException {
            this(in.readString(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        public SnapshotPolicyStats merge(SnapshotPolicyStats other) {
            return new SnapshotPolicyStats(
                this.policyId,
                this.snapshotsTaken + other.snapshotsTaken,
                this.snapshotsFailed + other.snapshotsFailed,
                this.snapshotsDeleted + other.snapshotsDeleted,
                this.snapshotDeleteFailures + other.snapshotDeleteFailures
            );
        }

        private static SnapshotPolicyStats taken(String policyId) {
            return new SnapshotPolicyStats(policyId, 1, 0, 0, 0);
        }

        private static SnapshotPolicyStats failed(String policyId) {
            return new SnapshotPolicyStats(policyId, 0, 1, 0, 0);
        }

        private static SnapshotPolicyStats deleted(String policyId) {
            return new SnapshotPolicyStats(policyId, 0, 0, 1, 0);
        }

        private static SnapshotPolicyStats deleteFailure(String policyId) {
            return new SnapshotPolicyStats(policyId, 0, 0, 0, 1);
        }

        public String getPolicyId() {
            return policyId;
        }

        public long getSnapshotTakenCount() {
            return snapshotsTaken;
        }

        public long getSnapshotFailedCount() {
            return snapshotsFailed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(policyId);
            out.writeVLong(snapshotsTaken);
            out.writeVLong(snapshotsFailed);
            out.writeVLong(snapshotsDeleted);
            out.writeVLong(snapshotDeleteFailures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policyId, snapshotsTaken, snapshotsFailed, snapshotsDeleted, snapshotDeleteFailures);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            SnapshotPolicyStats other = (SnapshotPolicyStats) obj;
            return Objects.equals(policyId, other.policyId)
                && Objects.equals(snapshotsTaken, other.snapshotsTaken)
                && Objects.equals(snapshotsFailed, other.snapshotsFailed)
                && Objects.equals(snapshotsDeleted, other.snapshotsDeleted)
                && Objects.equals(snapshotDeleteFailures, other.snapshotDeleteFailures);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SnapshotPolicyStats.POLICY_ID.getPreferredName(), policyId);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName(), snapshotsTaken);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_FAILED.getPreferredName(), snapshotsFailed);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_DELETED.getPreferredName(), snapshotsDeleted);
            builder.field(SnapshotPolicyStats.SNAPSHOT_DELETION_FAILURES.getPreferredName(), snapshotDeleteFailures);
            return builder;
        }
    }

}
