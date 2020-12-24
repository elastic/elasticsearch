/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SnapshotRetentionConfiguration implements ToXContentObject, Writeable {

    public static final SnapshotRetentionConfiguration EMPTY = new SnapshotRetentionConfiguration(null, null, null);

    private static final ParseField EXPIRE_AFTER = new ParseField("expire_after");
    private static final ParseField MINIMUM_SNAPSHOT_COUNT = new ParseField("min_count");
    private static final ParseField MAXIMUM_SNAPSHOT_COUNT = new ParseField("max_count");
    private static final Logger logger = LogManager.getLogger(SnapshotRetentionConfiguration.class);

    private static final ConstructingObjectParser<SnapshotRetentionConfiguration, Void> PARSER =
        new ConstructingObjectParser<>("snapshot_retention", true, a -> {
            TimeValue expireAfter = a[0] == null ? null : TimeValue.parseTimeValue((String) a[0], EXPIRE_AFTER.getPreferredName());
            Integer minCount = (Integer) a[1];
            Integer maxCount = (Integer) a[2];
            return new SnapshotRetentionConfiguration(expireAfter, minCount, maxCount);
        });

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

    public SnapshotRetentionConfiguration(@Nullable TimeValue expireAfter,
                                          @Nullable Integer minimumSnapshotCount,
                                          @Nullable Integer maximumSnapshotCount) {
        this(System::currentTimeMillis, expireAfter, minimumSnapshotCount, maximumSnapshotCount);
    }

    public SnapshotRetentionConfiguration(LongSupplier nowSupplier,
                                          @Nullable TimeValue expireAfter,
                                          @Nullable Integer minimumSnapshotCount,
                                          @Nullable Integer maximumSnapshotCount) {
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
            throw new IllegalArgumentException("minimum snapshot count " + this.minimumSnapshotCount +
                " cannot be larger than maximum snapshot count " + this.maximumSnapshotCount);
        }
    }

    public static SnapshotRetentionConfiguration parse(XContentParser parser, String name) {
        return PARSER.apply(parser, null);
    }

    public TimeValue getExpireAfter() {
        return this.expireAfter;
    }

    public Integer getMinimumSnapshotCount() {
        return this.minimumSnapshotCount;
    }

    public Integer getMaximumSnapshotCount() {
        return this.maximumSnapshotCount;
    }

    /**
     * Return a predicate by which a SnapshotInfo can be tested to see
     * whether it should be deleted according to this retention policy.
     * @param allSnapshots a list of all snapshot pertaining to this SLM policy and repository
     */
    public Predicate<SnapshotInfo> getSnapshotDeletionPredicate(final List<SnapshotInfo> allSnapshots) {
        final int totalSnapshotCount = allSnapshots.size();
        final List<SnapshotInfo> sortedSnapshots = allSnapshots.stream()
            .sorted(Comparator.comparingLong(SnapshotInfo::startTime))
            .collect(Collectors.toList());
        final long successfulSnapshotCount = allSnapshots.stream()
            .filter(snap -> SnapshotState.SUCCESS.equals(snap.state()))
            .count();
        final long newestSuccessfulTimestamp = allSnapshots.stream()
            .filter(snap -> SnapshotState.SUCCESS.equals(snap.state()))
            .mapToLong(SnapshotInfo::startTime)
            .max()
            .orElse(Long.MIN_VALUE);
        final Set<SnapshotState> unsuccessfulStates = Set.of(SnapshotState.FAILED, SnapshotState.PARTIAL);

        return si -> {
            final String snapName = si.snapshotId().getName();

            // First, if there's no expire_after and a more recent successful snapshot, we can delete all the failed ones
            if (this.expireAfter == null && unsuccessfulStates.contains(si.state()) && newestSuccessfulTimestamp > si.startTime()) {
                // There's no expire_after and there's a more recent successful snapshot, delete this failed one
                logger.trace("[{}]: ELIGIBLE as it is {} and there is a more recent successful snapshot", snapName, si.state());
                return true;
            }

            // Next, enforce the maximum count, if the size is over the maximum number of
            // snapshots, then allow the oldest N (where N is the number over the maximum snapshot
            // count) snapshots to be eligible for deletion
            if (this.maximumSnapshotCount != null) {
                if (successfulSnapshotCount > this.maximumSnapshotCount) {
                    final long snapsToDelete = successfulSnapshotCount - this.maximumSnapshotCount;
                    final boolean eligible = sortedSnapshots.stream()
                        .limit(snapsToDelete)
                        .anyMatch(s -> s.equals(si));

                    if (eligible) {
                        logger.trace("[{}]: ELIGIBLE as it is one of the {} oldest snapshots with " +
                                "{} non-failed snapshots ({} total), over the limit of {} maximum snapshots",
                            snapName, snapsToDelete, successfulSnapshotCount, totalSnapshotCount, this.maximumSnapshotCount);
                        return true;
                    } else {
                        logger.trace("[{}]: INELIGIBLE as it is not one of the {} oldest snapshots with " +
                                "{} non-failed snapshots ({} total), over the limit of {} maximum snapshots",
                            snapName, snapsToDelete, successfulSnapshotCount, totalSnapshotCount, this.maximumSnapshotCount);
                        return false;
                    }
                }
            }

            // Next check the minimum count, since that is a blanket requirement regardless of time,
            // if we haven't hit the minimum then we need to keep the snapshot regardless of
            // expiration time
            if (this.minimumSnapshotCount != null) {
                if (successfulSnapshotCount <= this.minimumSnapshotCount)
                    if (unsuccessfulStates.contains(si.state()) == false) {
                        logger.trace("[{}]: INELIGIBLE as there are {} non-failed snapshots ({} total) and {} minimum snapshots needed",
                            snapName, successfulSnapshotCount, totalSnapshotCount, this.minimumSnapshotCount);
                        return false;
                    } else {
                        logger.trace("[{}]: SKIPPING minimum snapshot count check as this snapshot is {} and not counted " +
                            "towards the minimum snapshot count.", snapName, si.state());
                    }
            }

            // Finally, check the expiration time of the snapshot, if it is past, then it is
            // eligible for deletion
            if (this.expireAfter != null) {
                final TimeValue snapshotAge = new TimeValue(nowSupplier.getAsLong() - si.startTime());

                if (this.minimumSnapshotCount != null) {
                    final long eligibleForExpiration = Math.max(0, successfulSnapshotCount - minimumSnapshotCount);

                    // Only the oldest N snapshots are actually eligible, since if we went below this we
                    // would fall below the configured minimum number of snapshots to keep
                    final Stream<SnapshotInfo> successfulSnapsEligibleForExpiration = sortedSnapshots.stream()
                        .filter(snap -> SnapshotState.SUCCESS.equals(snap.state()))
                        .limit(eligibleForExpiration);
                    final Stream<SnapshotInfo> unsucessfulSnaps = sortedSnapshots.stream()
                        .filter(snap -> unsuccessfulStates.contains(snap.state()));

                    final Set<SnapshotInfo> snapsEligibleForExpiration = Stream
                        .concat(successfulSnapsEligibleForExpiration, unsucessfulSnaps)
                        .collect(Collectors.toSet());

                    if (snapsEligibleForExpiration.contains(si) == false) {
                        // This snapshot is *not* one of the N oldest snapshots, so even if it were
                        // old enough, the other snapshots would be deleted before it
                        logger.trace("[{}]: INELIGIBLE as snapshot expiration would pass the " +
                                "minimum number of configured snapshots ({}) to keep, regardless of age",
                            snapName, this.minimumSnapshotCount);
                        return false;
                    }
                }

                if (snapshotAge.compareTo(this.expireAfter) > 0) {
                    logger.trace("[{}]: ELIGIBLE as snapshot age of {} is older than {}",
                        snapName, snapshotAge.toHumanReadableString(3), this.expireAfter.toHumanReadableString(3));
                    return true;
                } else {
                    logger.trace("[{}]: INELIGIBLE as snapshot age of {} is newer than {}",
                        snapName, snapshotAge.toHumanReadableString(3), this.expireAfter.toHumanReadableString(3));
                    return false;
                }
            }
            // If nothing matched, the snapshot is not eligible for deletion
            logger.trace("[{}]: INELIGIBLE as no retention predicates matched", snapName);
            return false;
        };
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
        return Objects.equals(this.expireAfter, other.expireAfter) &&
            Objects.equals(minimumSnapshotCount, other.minimumSnapshotCount) &&
            Objects.equals(maximumSnapshotCount, other.maximumSnapshotCount);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
