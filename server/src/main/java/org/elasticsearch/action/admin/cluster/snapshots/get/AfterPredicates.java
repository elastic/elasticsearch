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
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Predicate for the {@code ?after} filter of the get-snapshots action. The {@link #test(SnapshotInfo)} predicate is applied to
 * {@link SnapshotInfo} instances to filter out those that sort before the cursor value (i.e. were returned on earlier pages of results).
 */
final class AfterPredicates {

    private static final AfterPredicates MATCH_ALL = new AfterPredicates(null);

    @Nullable // if MATCH_ALL
    private final Predicate<SnapshotInfo> snapshotPredicate;

    private AfterPredicates(@Nullable Predicate<SnapshotInfo> snapshotPredicate) {
        this.snapshotPredicate = snapshotPredicate;
    }

    /**
     * Test using the full {@link SnapshotInfo}. Returns true if the snapshot should be included (sorts after the cursor).
     */
    boolean test(SnapshotInfo snapshotInfo) {
        return snapshotPredicate == null || snapshotPredicate.test(snapshotInfo);
    }

    static AfterPredicates forAfter(@Nullable After after, SnapshotSortKey sortBy, SortOrder order) {
        if (after == null) {
            return MATCH_ALL;
        }

        return new AfterPredicates(switch (sortBy) {
            case START_TIME -> longValuePredicate(after, SnapshotInfo::startTime, order);
            case NAME -> namePredicate(after, order);
            case DURATION -> longValuePredicate(after, info -> info.endTime() - info.startTime(), order);
            case INDICES -> longValuePredicate(after, info -> info.indices().size(), order);
            case SHARDS -> longValuePredicate(after, SnapshotInfo::totalShards, order);
            case FAILED_SHARDS -> longValuePredicate(after, SnapshotInfo::failedShards, order);
            case REPOSITORY -> repositoryPredicate(after, order);
        });
    }

    private static Predicate<SnapshotInfo> longValuePredicate(After after, ToLongFunction<SnapshotInfo> extractor, SortOrder sortOrder) {
        final long afterVal = Long.parseLong(after.value());
        final String snapshotName = after.snapshotName();
        final String repoName = after.repoName();
        return sortOrder == SortOrder.ASC ? info -> {
            final long val = extractor.applyAsLong(info);
            return afterVal < val || (afterVal == val && compareName(snapshotName, repoName, info) < 0);
        } : info -> {
            final long val = extractor.applyAsLong(info);
            return afterVal > val || (afterVal == val && compareName(snapshotName, repoName, info) > 0);
        };
    }

    private static Predicate<SnapshotInfo> namePredicate(After after, SortOrder sortOrder) {
        final String snapshotName = after.snapshotName();
        final String repoName = after.repoName();
        return sortOrder == SortOrder.ASC
            ? (info -> compareName(snapshotName, repoName, info) < 0)
            : (info -> compareName(snapshotName, repoName, info) > 0);
    }

    private static Predicate<SnapshotInfo> repositoryPredicate(After after, SortOrder sortOrder) {
        final String snapshotName = after.snapshotName();
        final String repoName = after.repoName();
        return sortOrder == SortOrder.ASC
            ? (info -> compareRepositoryName(snapshotName, repoName, info) < 0)
            : (info -> compareRepositoryName(snapshotName, repoName, info) > 0);
    }

    private static int compareName(String name, String repoName, SnapshotInfo info) {
        final int res = name.compareTo(info.snapshotId().getName());
        if (res != 0) {
            return res;
        }
        return repoName.compareTo(info.repository());
    }

    private static int compareRepositoryName(String name, String repoName, SnapshotInfo info) {
        final int res = repoName.compareTo(info.repository());
        if (res != 0) {
            return res;
        }
        return name.compareTo(info.snapshotId().getName());
    }
}
