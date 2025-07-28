/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class SnapshotRetentionConfiguration implements ToXContentObject, Writeable {

    public static final SnapshotRetentionConfiguration EMPTY = new SnapshotRetentionConfiguration(null, null, null);

    private static final ParseField EXPIRE_AFTER = new ParseField("expire_after");
    private static final ParseField MINIMUM_SNAPSHOT_COUNT = new ParseField("min_count");
    private static final ParseField MAXIMUM_SNAPSHOT_COUNT = new ParseField("max_count");
    private static final Logger logger = LogManager.getLogger(SnapshotRetentionConfiguration.class);

    private static final Set<SnapshotState> UNSUCCESSFUL_STATES = EnumSet.of(SnapshotState.FAILED, SnapshotState.PARTIAL);

    private static final ConstructingObjectParser<SnapshotRetentionConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "snapshot_retention",
        true,
        a -> {
            TimeValue expireAfter = a[0] == null ? null : TimeValue.parseTimeValue((String) a[0], EXPIRE_AFTER.getPreferredName());
            Integer minCount = (Integer) a[1];
            Integer maxCount = (Integer) a[2];
            return new SnapshotRetentionConfiguration(expireAfter, minCount, maxCount);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), EXPIRE_AFTER);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MINIMUM_SNAPSHOT_COUNT);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAXIMUM_SNAPSHOT_COUNT);
    }

    private final LongSupplier nowSupplier;
    private final TimeValue expireAfter;
    private final Integer minimumSnapshotCount;
    private final Integer maximumSnapshotCount;

    SnapshotRetentionConfiguration(StreamInput in) throws IOException {
        nowSupplier = System::currentTimeMillis;
        this.expireAfter = in.readOptionalTimeValue();
        this.minimumSnapshotCount = in.readOptionalVInt();
        this.maximumSnapshotCount = in.readOptionalVInt();
    }

    public SnapshotRetentionConfiguration(
        @Nullable TimeValue expireAfter,
        @Nullable Integer minimumSnapshotCount,
        @Nullable Integer maximumSnapshotCount
    ) {
        this(System::currentTimeMillis, expireAfter, minimumSnapshotCount, maximumSnapshotCount);
    }

    public SnapshotRetentionConfiguration(
        LongSupplier nowSupplier,
        @Nullable TimeValue expireAfter,
        @Nullable Integer minimumSnapshotCount,
        @Nullable Integer maximumSnapshotCount
    ) {
        this.nowSupplier = nowSupplier;
        this.expireAfter = expireAfter;
        this.minimumSnapshotCount = minimumSnapshotCount;
        this.maximumSnapshotCount = maximumSnapshotCount;
        if (this.minimumSnapshotCount != null && this.minimumSnapshotCount < 1) {
            throw new IllegalArgumentException("minimum snapshot count must be at least 1, but was: " + this.minimumSnapshotCount);
        }
        if (this.maximumSnapshotCount != null && this.maximumSnapshotCount < 1) {
            throw new IllegalArgumentException("maximum snapshot count must be at least 1, but was: " + this.maximumSnapshotCount);
        }
        if ((maximumSnapshotCount != null && minimumSnapshotCount != null) && this.minimumSnapshotCount > this.maximumSnapshotCount) {
            throw new IllegalArgumentException(
                "minimum snapshot count "
                    + this.minimumSnapshotCount
                    + " cannot be larger than maximum snapshot count "
                    + this.maximumSnapshotCount
            );
        }
    }

    public static SnapshotRetentionConfiguration parse(XContentParser parser, String name) {
        return PARSER.apply(parser, null);
    }

    /**
     * @return whether a snapshot should be deleted according to this retention policy.
     * @param allSnapshots all the snapshot details pertaining to this SLM policy and repository
     */
    public boolean isSnapshotEligibleForDeletion(
        SnapshotId snapshotId,
        RepositoryData.SnapshotDetails snapshotDetails,
        Map<SnapshotId, RepositoryData.SnapshotDetails> allSnapshots
    ) {
        assert Strings.hasText(snapshotDetails.getSlmPolicy());
        final var snapshotState = snapshotDetails.getSnapshotState();
        final var startTimeMillis = snapshotDetails.getStartTimeMillis();
        final var snapshotName = snapshotId.getName();

        final int totalSnapshotCount = allSnapshots.size();
        final var sortedSnapshots = allSnapshots.entrySet()
            .stream()
            .sorted(Comparator.comparingLong(e -> e.getValue().getStartTimeMillis()))
            .toList();
        int successCount = 0;
        long latestSuccessfulTimestamp = Long.MIN_VALUE;
        for (final var snapshot : allSnapshots.values()) {
            assert Objects.equals(snapshot.getSlmPolicy(), snapshotDetails.getSlmPolicy());
            if (snapshot.getSnapshotState() == SnapshotState.SUCCESS) {
                successCount++;
                latestSuccessfulTimestamp = Math.max(latestSuccessfulTimestamp, snapshot.getStartTimeMillis());
            }
        }
        final long newestSuccessfulTimestamp = latestSuccessfulTimestamp;
        final int successfulSnapshotCount = successCount;

        // First, if there's no expire_after and a more recent successful snapshot, we can delete all the failed ones
        if (this.expireAfter == null && UNSUCCESSFUL_STATES.contains(snapshotState) && newestSuccessfulTimestamp > startTimeMillis) {
            // There's no expire_after and there's a more recent successful snapshot, delete this failed one
            logger.trace("[{}]: ELIGIBLE as it is {} and there is a more recent successful snapshot", snapshotName, snapshotState);
            return true;
        }

        // Next, enforce the maximum count, if the size is over the maximum number of
        // snapshots, then allow the oldest N (where N is the number over the maximum snapshot
        // count) snapshots to be eligible for deletion
        if (this.maximumSnapshotCount != null && successfulSnapshotCount > this.maximumSnapshotCount) {
            final long successfulSnapsToDelete = successfulSnapshotCount - this.maximumSnapshotCount;
            boolean found = false;
            int successfulSeen = 0;
            for (final var s : sortedSnapshots) {
                if (s.getValue().getSnapshotState() == SnapshotState.SUCCESS) {
                    successfulSeen++;
                }
                if (successfulSeen > successfulSnapsToDelete) {
                    break;
                }
                if (s.getKey().equals(snapshotId)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                logger.trace(
                    "[{}]: ELIGIBLE as it is one of the {} oldest snapshots with "
                        + "{} non-failed snapshots ({} total), over the limit of {} maximum snapshots",
                    snapshotName,
                    successfulSnapsToDelete,
                    successfulSnapshotCount,
                    totalSnapshotCount,
                    this.maximumSnapshotCount
                );
                return true;
            } else {
                logger.trace(
                    "[{}]: SKIPPING as it is not one of the {} oldest snapshots with "
                        + "{} non-failed snapshots ({} total), over the limit of {} maximum snapshots",
                    snapshotName,
                    successfulSnapsToDelete,
                    successfulSnapshotCount,
                    totalSnapshotCount,
                    this.maximumSnapshotCount
                );
            }
        }

        // Next check the minimum count, since that is a blanket requirement regardless of time,
        // if we haven't hit the minimum then we need to keep the snapshot regardless of
        // expiration time
        if (this.minimumSnapshotCount != null && successfulSnapshotCount <= this.minimumSnapshotCount) {
            if (UNSUCCESSFUL_STATES.contains(snapshotState) == false) {
                logger.trace(
                    "[{}]: INELIGIBLE as there are {} non-failed snapshots ({} total) and {} minimum snapshots needed",
                    snapshotName,
                    successfulSnapshotCount,
                    totalSnapshotCount,
                    this.minimumSnapshotCount
                );
                return false;
            } else {
                logger.trace(
                    "[{}]: SKIPPING minimum snapshot count check as this snapshot is {} and not counted "
                        + "towards the minimum snapshot count.",
                    snapshotName,
                    snapshotState
                );
            }
        }

        // Finally, check the expiration time of the snapshot, if it is past, then it is
        // eligible for deletion
        if (this.expireAfter != null) {
            if (this.minimumSnapshotCount != null) {
                // Only the oldest N snapshots are actually eligible, since if we went below this we
                // would fall below the configured minimum number of snapshots to keep
                final boolean maybeEligible;
                if (snapshotState == SnapshotState.SUCCESS) {
                    maybeEligible = sortedSnapshots.stream()
                        .filter(snap -> SnapshotState.SUCCESS.equals(snap.getValue().getSnapshotState()))
                        .limit(Math.max(0, successfulSnapshotCount - minimumSnapshotCount))
                        .anyMatch(s -> s.getKey().equals(snapshotId));
                } else if (UNSUCCESSFUL_STATES.contains(snapshotState)) {
                    maybeEligible = allSnapshots.containsKey(snapshotId);
                } else {
                    logger.trace("[{}] INELIGIBLE because snapshot is in state [{}]", snapshotName, snapshotState);
                    return false;
                }
                if (maybeEligible == false) {
                    // This snapshot is *not* one of the N oldest snapshots, so even if it were
                    // old enough, the other snapshots would be deleted before it
                    logger.trace(
                        "[{}]: INELIGIBLE as snapshot expiration would pass the "
                            + "minimum number of configured snapshots ({}) to keep, regardless of age",
                        snapshotName,
                        this.minimumSnapshotCount
                    );
                    return false;
                }
            }
            final long snapshotAge = nowSupplier.getAsLong() - startTimeMillis;
            if (snapshotAge > this.expireAfter.getMillis()) {
                logger.trace(
                    () -> format(
                        "[%s]: ELIGIBLE as snapshot age of %s is older than %s",
                        snapshotName,
                        new TimeValue(snapshotAge).toHumanReadableString(3),
                        this.expireAfter.toHumanReadableString(3)
                    )
                );
                return true;
            } else {
                logger.trace(
                    () -> format(
                        "[%s]: INELIGIBLE as snapshot age of [%sms] is newer than %s",
                        snapshotName,
                        new TimeValue(snapshotAge).toHumanReadableString(3),
                        this.expireAfter.toHumanReadableString(3)
                    )
                );
                return false;
            }
        }
        // If nothing matched, the snapshot is not eligible for deletion
        logger.trace("[{}]: INELIGIBLE as no retention predicates matched", snapshotName);
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(this.expireAfter);
        out.writeOptionalVInt(this.minimumSnapshotCount);
        out.writeOptionalVInt(this.maximumSnapshotCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (expireAfter != null) {
            builder.field(EXPIRE_AFTER.getPreferredName(), expireAfter.getStringRep());
        }
        if (minimumSnapshotCount != null) {
            builder.field(MINIMUM_SNAPSHOT_COUNT.getPreferredName(), minimumSnapshotCount);
        }
        if (maximumSnapshotCount != null) {
            builder.field(MAXIMUM_SNAPSHOT_COUNT.getPreferredName(), maximumSnapshotCount);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expireAfter, minimumSnapshotCount, maximumSnapshotCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotRetentionConfiguration other = (SnapshotRetentionConfiguration) obj;
        return Objects.equals(this.expireAfter, other.expireAfter)
            && Objects.equals(minimumSnapshotCount, other.minimumSnapshotCount)
            && Objects.equals(maximumSnapshotCount, other.maximumSnapshotCount);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
