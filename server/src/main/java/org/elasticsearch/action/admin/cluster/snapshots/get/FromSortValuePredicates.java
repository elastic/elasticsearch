/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.EXCLUDE;
import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.INCLUDE;
import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.INCONCLUSIVE;

/**
 * A pair of predicates for the {@code ?from_sort_value} filter of the get-snapshots action. The {@link #test(SnapshotId, RepositoryData)}
 * predicate is applied to combinations of snapshot id and repository data to determine which snapshots to fully load from the repository
 * and rules out all snapshots that do not match the given {@link GetSnapshotsRequest} that can be ruled out through the information in
 * {@link RepositoryData}. The predicate returned by {@link #test(SnapshotInfo)} predicate is then applied the instances of
 * {@link SnapshotInfo} that were loaded from the repository to filter out those remaining that did not match the request but could not be
 * ruled out without loading their {@link SnapshotInfo}.
 */
final class FromSortValuePredicates {

    private static final FromSortValuePredicates MATCH_ALL = new FromSortValuePredicates(null, null);

    @Nullable // non-null for sort keys that can be filtered from repository data (NAME, INDICES, START_TIME, DURATION)
    private final PreflightFilterResult.RepositoryDataFilter preflightPredicate;

    @Nullable // null if all snapshots match or the pre-flight filter is guaranteed to be conclusive so no SnapshotInfo filter needed
    private final Predicate<SnapshotInfo> snapshotPredicate;

    private FromSortValuePredicates(
        @Nullable PreflightFilterResult.RepositoryDataFilter preflightPredicate,
        @Nullable Predicate<SnapshotInfo> snapshotPredicate
    ) {
        this.snapshotPredicate = snapshotPredicate;
        this.preflightPredicate = preflightPredicate;
    }

    /**
     * Pre-flight test using only snapshot id and repository data, i.e. without needing to load the full {@link SnapshotInfo}.
     *
     * @return an accurate result for {@link SnapshotSortKey#NAME}, {@link SnapshotSortKey#INDICES} and
     * {@link SnapshotSortKey#REPOSITORY} (the latter because {@code GetSnapshotsOperation#skipRepository} handles this case even
     * earlier). Also accurate for {@link SnapshotSortKey#START_TIME} and {@link SnapshotSortKey#DURATION} if and only if the
     * corresponding values are available in {@link RepositoryData.SnapshotDetails}; returns {@link PreflightFilterResult#INCONCLUSIVE}
     * otherwise.
     * @see PreflightFilterResult.RepositoryDataFilter
     */
    PreflightFilterResult test(SnapshotId snapshotId, RepositoryData repositoryData) {
        if (this == MATCH_ALL) {
            // no ?from_sort_value parameter, or sorting by REPOSITORY
            return INCLUDE;
        }
        if (preflightPredicate == null) {
            // ?from_sort_value specified, and we are sorting by SHARDS or FAILED_SHARDS that requires the full SnapshotInfo
            return INCONCLUSIVE;
        }
        return preflightPredicate.test(snapshotId, repositoryData);
    }

    boolean isMatchAll() {
        return this == MATCH_ALL;
    }

    /**
     * Test using the full {@link SnapshotInfo}.
     *
     * @return an accurate result for {@link SnapshotSortKey#START_TIME} and {@link SnapshotSortKey#DURATION} (in case the pre-flight
     * test was inconclusive) and for {@link SnapshotSortKey#SHARDS} and {@link SnapshotSortKey#FAILED_SHARDS} which are not available
     * without loading the {@link SnapshotInfo}. Note that {@link SnapshotSortKey#NAME} and {@link SnapshotSortKey#INDICES} can always
     * be checked accurately before loading the {@link SnapshotInfo} so they are not checked again here.
     */
    boolean test(SnapshotInfo snapshotInfo) {
        return snapshotPredicate == null || snapshotPredicate.test(snapshotInfo);
    }

    static FromSortValuePredicates forFromSortValue(String fromSortValue, SnapshotSortKey sortBy, SortOrder order) {
        if (fromSortValue == null) {
            return MATCH_ALL;
        }

        return switch (sortBy) {
            case START_TIME -> {
                final long after = Long.parseLong(fromSortValue);
                yield new FromSortValuePredicates((snapshotId, repositoryData) -> {
                    final long startTime = getStartTime(snapshotId, repositoryData);
                    if (startTime == -1) {
                        return INCONCLUSIVE;
                    }
                    return order == SortOrder.ASC ? (after <= startTime ? INCLUDE : EXCLUDE) : (after >= startTime ? INCLUDE : EXCLUDE);
                }, filterByLongOffset(SnapshotInfo::startTime, after, order));
            }
            case NAME -> new FromSortValuePredicates(
                order == SortOrder.ASC
                    ? (snapshotId, ignoredRepositoryData) -> fromSortValue.compareTo(snapshotId.getName()) <= 0 ? INCLUDE : EXCLUDE
                    : (snapshotId, ignoredRepositoryData) -> fromSortValue.compareTo(snapshotId.getName()) >= 0 ? INCLUDE : EXCLUDE,
                null
            );
            case DURATION -> {
                final long afterDuration = Long.parseLong(fromSortValue);
                yield new FromSortValuePredicates((snapshotId, repositoryData) -> {
                    final long duration = getDuration(snapshotId, repositoryData);
                    if (duration == -1) {
                        return INCONCLUSIVE;
                    }
                    return order == SortOrder.ASC
                        ? (afterDuration <= duration ? INCLUDE : EXCLUDE)
                        : (afterDuration >= duration ? INCLUDE : EXCLUDE);
                }, filterByLongOffset(info -> info.endTime() - info.startTime(), afterDuration, order));
            }
            case INDICES -> {
                final int afterIndexCount = Integer.parseInt(fromSortValue);
                yield new FromSortValuePredicates(
                    order == SortOrder.ASC
                        ? (snapshotId, repositoryData) -> afterIndexCount <= indexCount(snapshotId, repositoryData) ? INCLUDE : EXCLUDE
                        : (snapshotId, repositoryData) -> afterIndexCount >= indexCount(snapshotId, repositoryData) ? INCLUDE : EXCLUDE,
                    null
                );
            }
            case REPOSITORY -> MATCH_ALL; // already handled in GetSnapshotsOperation#skipRepository
            case SHARDS -> new FromSortValuePredicates(
                null,
                filterByLongOffset(SnapshotInfo::totalShards, Integer.parseInt(fromSortValue), order)
            );
            case FAILED_SHARDS -> new FromSortValuePredicates(
                null,
                filterByLongOffset(SnapshotInfo::failedShards, Integer.parseInt(fromSortValue), order)
            );
        };
    }

    private static Predicate<SnapshotInfo> filterByLongOffset(ToLongFunction<SnapshotInfo> extractor, long after, SortOrder order) {
        return order == SortOrder.ASC ? info -> after <= extractor.applyAsLong(info) : info -> after >= extractor.applyAsLong(info);
    }

    private static long getDuration(SnapshotId snapshotId, RepositoryData repositoryData) {
        final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
        if (details == null) {
            return -1;
        }
        final long startTime = details.getStartTimeMillis();
        if (startTime == -1) {
            return -1;
        }
        final long endTime = details.getEndTimeMillis();
        if (endTime == -1) {
            return -1;
        }
        return endTime - startTime;
    }

    private static long getStartTime(SnapshotId snapshotId, RepositoryData repositoryData) {
        final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
        return details == null ? -1 : details.getStartTimeMillis();
    }

    private static int indexCount(SnapshotId snapshotId, RepositoryData repositoryData) {
        // TODO: this could be made more efficient by caching this number in RepositoryData
        int indexCount = 0;
        for (IndexId idx : repositoryData.getIndices().values()) {
            if (repositoryData.getSnapshots(idx).contains(snapshotId)) {
                indexCount++;
            }
        }
        return indexCount;
    }
}
