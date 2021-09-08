/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SnapshotLifecycleStats contains metrics and stats about snapshot lifecycle policy execution - how
 * many snapshots were taken, deleted, how many failures, etc. It contains both global stats
 * (snapshots taken, retention runs), and per-policy stats.
 */
public class SnapshotLifecycleStats implements Writeable, ToXContentObject {

    private final CounterMetric retentionRunCount = new CounterMetric();
    private final CounterMetric retentionFailedCount = new CounterMetric();
    private final CounterMetric retentionTimedOut = new CounterMetric();
    private final CounterMetric retentionTimeMs = new CounterMetric();
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
    private static final ConstructingObjectParser<SnapshotLifecycleStats, Void> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_stats", true,
            a -> {
                long runs = (long) a[0];
                long failed = (long) a[1];
                long timedOut = (long) a[2];
                long timeMs = (long) a[3];
                Map<String, SnapshotPolicyStats> policyStatsMap = ((List<SnapshotPolicyStats>) a[4]).stream()
                    .collect(Collectors.toMap(m -> m.policyId, Function.identity()));
                return new SnapshotLifecycleStats(runs, failed, timedOut, timeMs, policyStatsMap);
            });

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_RUNS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_FAILED);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_TIMED_OUT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), RETENTION_TIME_MILLIS);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), SnapshotPolicyStats.PARSER, POLICY_STATS);
    }

    public SnapshotLifecycleStats() {
        this.policyStats = new ConcurrentHashMap<>();
    }

    // public for testing
    public SnapshotLifecycleStats(long retentionRuns, long retentionFailed, long retentionTimedOut, long retentionTimeMs,
                           Map<String, SnapshotPolicyStats> policyStats) {
        this.retentionRunCount.inc(retentionRuns);
        this.retentionFailedCount.inc(retentionFailed);
        this.retentionTimedOut.inc(retentionTimedOut);
        this.retentionTimeMs.inc(retentionTimeMs);
        this.policyStats = policyStats;
    }

    public SnapshotLifecycleStats(StreamInput in) throws IOException {
        this.policyStats = new ConcurrentHashMap<>(in.readMap(StreamInput::readString, SnapshotPolicyStats::new));
        this.retentionRunCount.inc(in.readVLong());
        this.retentionFailedCount.inc(in.readVLong());
        this.retentionTimedOut.inc(in.readVLong());
        this.retentionTimeMs.inc(in.readVLong());
    }

    public static SnapshotLifecycleStats parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SnapshotLifecycleStats merge(SnapshotLifecycleStats other) {

        HashMap<String, SnapshotPolicyStats> newPolicyStats = new HashMap<>(this.policyStats);
        // Merges the per-run stats (the stats in "other") with the stats already present
        other.policyStats
            .forEach((policyId, perRunPolicyStats) -> {
                newPolicyStats.compute(policyId, (k, existingPolicyMetrics) -> {
                    if (existingPolicyMetrics == null) {
                        return perRunPolicyStats;
                    } else {
                        return existingPolicyMetrics.merge(perRunPolicyStats);
                    }
                });
            });

        return new SnapshotLifecycleStats(this.retentionRunCount.count() + other.retentionRunCount.count(),
            this.retentionFailedCount.count() + other.retentionFailedCount.count(),
            this.retentionTimedOut.count() + other.retentionTimedOut.count(),
            this.retentionTimeMs.count() + other.retentionTimeMs.count(),
            newPolicyStats);
    }

    public SnapshotLifecycleStats removePolicy(String policyId) {
        Map<String, SnapshotPolicyStats> policyStats = new HashMap<>(this.policyStats);
        policyStats.remove(policyId);
        return new SnapshotLifecycleStats(this.retentionRunCount.count(), this.retentionFailedCount.count(),
            this.retentionTimedOut.count(), this.retentionTimeMs.count(),
            policyStats);
    }

    /**
     * @return a map of per-policy stats for each SLM policy
     */
    public Map<String, SnapshotPolicyStats> getMetrics() {
        return Collections.unmodifiableMap(this.policyStats);
    }

    /**
     * Increment the number of times SLM retention has been run
     */
    public void retentionRun() {
        this.retentionRunCount.inc();
    }

    /**
     * Increment the number of times SLM retention has failed
     */
    public void retentionFailed() {
        this.retentionFailedCount.inc();
    }

    /**
     * Increment the number of times that SLM retention timed out due to the max delete time
     * window being exceeded.
     */
    public void retentionTimedOut() {
        this.retentionTimedOut.inc();
    }

    /**
     * Register the amount of time taken for deleting snapshots during SLM retention
     */
    public void deletionTime(TimeValue elapsedTime) {
        this.retentionTimeMs.inc(elapsedTime.millis());
    }

    /**
     * Increment the per-policy snapshot taken count for the given policy id
     */
    public void snapshotTaken(String slmPolicy) {
        this.policyStats.computeIfAbsent(slmPolicy, SnapshotPolicyStats::new).snapshotTaken();
    }

    /**
     * Increment the per-policy snapshot failure count for the given policy id
     */
    public void snapshotFailed(String slmPolicy) {
        this.policyStats.computeIfAbsent(slmPolicy, SnapshotPolicyStats::new).snapshotFailed();
    }

    /**
     * Increment the per-policy snapshot deleted count for the given policy id
     */
    public void snapshotDeleted(String slmPolicy) {
        this.policyStats.computeIfAbsent(slmPolicy, SnapshotPolicyStats::new).snapshotDeleted();
    }

    /**
     * Increment the per-policy snapshot deletion failure count for the given policy id
     */
    public void snapshotDeleteFailure(String slmPolicy) {
        this.policyStats.computeIfAbsent(slmPolicy, SnapshotPolicyStats::new).snapshotDeleteFailure();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(policyStats, StreamOutput::writeString, (v, o) -> o.writeTo(v));
        out.writeVLong(retentionRunCount.count());
        out.writeVLong(retentionFailedCount.count());
        out.writeVLong(retentionTimedOut.count());
        out.writeVLong(retentionTimeMs.count());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RETENTION_RUNS.getPreferredName(), this.retentionRunCount.count());
        builder.field(RETENTION_FAILED.getPreferredName(), this.retentionFailedCount.count());
        builder.field(RETENTION_TIMED_OUT.getPreferredName(), this.retentionTimedOut.count());
        TimeValue retentionTime = TimeValue.timeValueMillis(this.retentionTimeMs.count());
        builder.field(RETENTION_TIME.getPreferredName(), retentionTime);
        builder.field(RETENTION_TIME_MILLIS.getPreferredName(), retentionTime.millis());

        List<SnapshotPolicyStats> metrics = getMetrics().values().stream()
            .sorted(Comparator.comparing(SnapshotPolicyStats::getPolicyId)) // maintain a consistent order when serializing
            .collect(Collectors.toList());
        long totalTaken = metrics.stream().mapToLong(s -> s.snapshotsTaken.count()).sum();
        long totalFailed = metrics.stream().mapToLong(s -> s.snapshotsFailed.count()).sum();
        long totalDeleted = metrics.stream().mapToLong(s -> s.snapshotsDeleted.count()).sum();
        long totalDeleteFailures = metrics.stream().mapToLong(s -> s.snapshotDeleteFailures.count()).sum();
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
        return Objects.hash(retentionRunCount.count(), retentionFailedCount.count(),
            retentionTimedOut.count(), retentionTimeMs.count(), policyStats);
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
        return Objects.equals(retentionRunCount.count(), other.retentionRunCount.count()) &&
            Objects.equals(retentionFailedCount.count(), other.retentionFailedCount.count()) &&
            Objects.equals(retentionTimedOut.count(), other.retentionTimedOut.count()) &&
            Objects.equals(retentionTimeMs.count(), other.retentionTimeMs.count()) &&
            Objects.equals(policyStats, other.policyStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class SnapshotPolicyStats implements Writeable, ToXContentFragment {
        private final String policyId;
        private final CounterMetric snapshotsTaken = new CounterMetric();
        private final CounterMetric snapshotsFailed = new CounterMetric();
        private final CounterMetric snapshotsDeleted = new CounterMetric();
        private final CounterMetric snapshotDeleteFailures = new CounterMetric();

        public static final ParseField POLICY_ID = new ParseField("policy");
        public static final ParseField SNAPSHOTS_TAKEN = new ParseField("snapshots_taken");
        public static final ParseField SNAPSHOTS_FAILED = new ParseField("snapshots_failed");
        public static final ParseField SNAPSHOTS_DELETED = new ParseField("snapshots_deleted");
        public static final ParseField SNAPSHOT_DELETION_FAILURES = new ParseField("snapshot_deletion_failures");

        static final ConstructingObjectParser<SnapshotPolicyStats, Void> PARSER =
            new ConstructingObjectParser<>("snapshot_policy_stats", true,
                a -> {
                    String id = (String) a[0];
                    long taken = (long) a[1];
                    long failed = (long) a[2];
                    long deleted = (long) a[3];
                    long deleteFailed = (long) a[4];
                    return new SnapshotPolicyStats(id, taken, failed, deleted, deleteFailed);
                });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_TAKEN);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_FAILED);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOTS_DELETED);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), SNAPSHOT_DELETION_FAILURES);
        }

        public SnapshotPolicyStats(String slmPolicy) {
            this.policyId = slmPolicy;
        }

        public SnapshotPolicyStats(String policyId, long snapshotsTaken, long snapshotsFailed, long deleted, long failedDeletes) {
            this.policyId = policyId;
            this.snapshotsTaken.inc(snapshotsTaken);
            this.snapshotsFailed.inc(snapshotsFailed);
            this.snapshotsDeleted.inc(deleted);
            this.snapshotDeleteFailures.inc(failedDeletes);
        }

        public SnapshotPolicyStats(StreamInput in) throws IOException {
            this.policyId = in.readString();
            this.snapshotsTaken.inc(in.readVLong());
            this.snapshotsFailed.inc(in.readVLong());
            this.snapshotsDeleted.inc(in.readVLong());
            this.snapshotDeleteFailures.inc(in.readVLong());
        }

        public static SnapshotPolicyStats parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public SnapshotPolicyStats merge(SnapshotPolicyStats other) {
            return new SnapshotPolicyStats(
                this.policyId,
                this.snapshotsTaken.count() + other.snapshotsTaken.count(),
                this.snapshotsFailed.count() + other.snapshotsFailed.count(),
                this.snapshotsDeleted.count() + other.snapshotsDeleted.count(),
                this.snapshotDeleteFailures.count() + other.snapshotDeleteFailures.count());
        }

        void snapshotTaken() {
            snapshotsTaken.inc();
        }

        void snapshotFailed() {
            snapshotsFailed.inc();
        }

        void snapshotDeleted() {
            snapshotsDeleted.inc();
        }

        void snapshotDeleteFailure() {
            snapshotDeleteFailures.inc();
        }

        public String getPolicyId() {
            return policyId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(policyId);
            out.writeVLong(snapshotsTaken.count());
            out.writeVLong(snapshotsFailed.count());
            out.writeVLong(snapshotsDeleted.count());
            out.writeVLong(snapshotDeleteFailures.count());
        }

        @Override
        public int hashCode() {
            return Objects.hash(policyId, snapshotsTaken.count(), snapshotsFailed.count(),
                snapshotsDeleted.count(), snapshotDeleteFailures.count());
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
            return Objects.equals(policyId, other.policyId) &&
                Objects.equals(snapshotsTaken.count(), other.snapshotsTaken.count()) &&
                Objects.equals(snapshotsFailed.count(), other.snapshotsFailed.count()) &&
                Objects.equals(snapshotsDeleted.count(), other.snapshotsDeleted.count()) &&
                Objects.equals(snapshotDeleteFailures.count(), other.snapshotDeleteFailures.count());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SnapshotPolicyStats.POLICY_ID.getPreferredName(), policyId);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName(), snapshotsTaken.count());
            builder.field(SnapshotPolicyStats.SNAPSHOTS_FAILED.getPreferredName(), snapshotsFailed.count());
            builder.field(SnapshotPolicyStats.SNAPSHOTS_DELETED.getPreferredName(), snapshotsDeleted.count());
            builder.field(SnapshotPolicyStats.SNAPSHOT_DELETION_FAILURES.getPreferredName(), snapshotDeleteFailures.count());
            return builder;
        }
    }

}
