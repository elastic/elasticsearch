/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SnapshotLifecycleStats implements ToXContentObject {

    private final long retentionRunCount;
    private final long retentionFailedCount;
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

    // Package visible for testing
    private SnapshotLifecycleStats(long retentionRuns, long retentionFailed, long retentionTimedOut, long retentionTimeMs,
                                   Map<String, SnapshotPolicyStats> policyStats) {
        this.retentionRunCount = retentionRuns;
        this.retentionFailedCount = retentionFailed;
        this.retentionTimedOut = retentionTimedOut;
        this.retentionTimeMs = retentionTimeMs;
        this.policyStats = policyStats;
    }

    public static SnapshotLifecycleStats parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public long getRetentionRunCount() {
        return retentionRunCount;
    }

    public long getRetentionFailedCount() {
        return retentionFailedCount;
    }

    public long getRetentionTimedOut() {
        return retentionTimedOut;
    }

    public long getRetentionTimeMillis() {
        return retentionTimeMs;
    }

    /**
     * @return a map of per-policy stats for each SLM policy
     */
    public Map<String, SnapshotPolicyStats> getMetrics() {
        return Collections.unmodifiableMap(this.policyStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RETENTION_RUNS.getPreferredName(), this.retentionRunCount);
        builder.field(RETENTION_FAILED.getPreferredName(), this.retentionFailedCount);
        builder.field(RETENTION_TIMED_OUT.getPreferredName(), this.retentionTimedOut);
        TimeValue retentionTime = TimeValue.timeValueMillis(this.retentionTimeMs);
        builder.field(RETENTION_TIME.getPreferredName(), retentionTime);
        builder.field(RETENTION_TIME_MILLIS.getPreferredName(), retentionTime.millis());

        Map<String, SnapshotPolicyStats> metrics = getMetrics();
        long totalTaken = metrics.values().stream().mapToLong(s -> s.snapshotsTaken).sum();
        long totalFailed = metrics.values().stream().mapToLong(s -> s.snapshotsFailed).sum();
        long totalDeleted = metrics.values().stream().mapToLong(s -> s.snapshotsDeleted).sum();
        long totalDeleteFailures = metrics.values().stream().mapToLong(s -> s.snapshotDeleteFailures).sum();
        builder.field(TOTAL_TAKEN.getPreferredName(), totalTaken);
        builder.field(TOTAL_FAILED.getPreferredName(), totalFailed);
        builder.field(TOTAL_DELETIONS.getPreferredName(), totalDeleted);
        builder.field(TOTAL_DELETION_FAILURES.getPreferredName(), totalDeleteFailures);
        builder.startObject(POLICY_STATS.getPreferredName());
        for (Map.Entry<String, SnapshotPolicyStats> policy : metrics.entrySet()) {
            SnapshotPolicyStats perPolicyMetrics = policy.getValue();
            builder.startObject(perPolicyMetrics.policyId);
            perPolicyMetrics.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(retentionRunCount, retentionFailedCount, retentionTimedOut, retentionTimeMs, policyStats);
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
        return retentionRunCount == other.retentionRunCount &&
            retentionFailedCount == other.retentionFailedCount &&
            retentionTimedOut == other.retentionTimedOut &&
            retentionTimeMs == other.retentionTimeMs &&
            Objects.equals(policyStats, other.policyStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class SnapshotPolicyStats implements ToXContentFragment {
        private final String policyId;
        private final long snapshotsTaken;
        private final long snapshotsFailed;
        private final long snapshotsDeleted;
        private final long snapshotDeleteFailures;

        public static final ParseField POLICY_ID = new ParseField("policy");
        static final ParseField SNAPSHOTS_TAKEN = new ParseField("snapshots_taken");
        static final ParseField SNAPSHOTS_FAILED = new ParseField("snapshots_failed");
        static final ParseField SNAPSHOTS_DELETED = new ParseField("snapshots_deleted");
        static final ParseField SNAPSHOT_DELETION_FAILURES = new ParseField("snapshot_deletion_failures");

        private static final ConstructingObjectParser<SnapshotPolicyStats, Void> PARSER =
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

        public SnapshotPolicyStats(String policyId, long snapshotsTaken, long snapshotsFailed, long deleted, long failedDeletes) {
            this.policyId = policyId;
            this.snapshotsTaken = snapshotsTaken;
            this.snapshotsFailed = snapshotsFailed;
            this.snapshotsDeleted = deleted;
            this.snapshotDeleteFailures = failedDeletes;
        }

        public static SnapshotPolicyStats parse(XContentParser parser, String policyId) {
            return PARSER.apply(parser, null);
        }

        public String getPolicyId() {
            return policyId;
        }

        public long getSnapshotsTaken() {
            return snapshotsTaken;
        }

        public long getSnapshotsFailed() {
            return snapshotsFailed;
        }

        public long getSnapshotsDeleted() {
            return snapshotsDeleted;
        }

        public long getSnapshotDeleteFailures() {
            return snapshotDeleteFailures;
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
            return Objects.equals(policyId, other.policyId) &&
                snapshotsTaken == other.snapshotsTaken &&
                snapshotsFailed == other.snapshotsFailed &&
                snapshotsDeleted == other.snapshotsDeleted &&
                snapshotDeleteFailures == other.snapshotDeleteFailures;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName(), snapshotsTaken);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_FAILED.getPreferredName(), snapshotsFailed);
            builder.field(SnapshotPolicyStats.SNAPSHOTS_DELETED.getPreferredName(), snapshotsDeleted);
            builder.field(SnapshotPolicyStats.SNAPSHOT_DELETION_FAILURES.getPreferredName(), snapshotDeleteFailures);
            return builder;
        }
    }
}
