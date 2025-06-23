/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Sort key for snapshots e.g. returned from the get-snapshots API. All values break ties using {@link SnapshotInfo#snapshotId} (i.e. by
 * name).
 */
public enum SnapshotSortKey {
    /**
     * Sort by snapshot start time.
     */
    START_TIME("start_time", Comparator.comparingLong(SnapshotInfo::startTime)) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return Long.toString(snapshotInfo.startTime());
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            return after.longValuePredicate(SnapshotInfo::startTime, sortOrder);
        }
    },

    /**
     * Sort by snapshot name.
     */
    NAME("name", Comparator.comparing(sni -> sni.snapshotId().getName())) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return snapshotInfo.snapshotId().getName();
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            // TODO: cover via pre-flight predicate
            final String snapshotName = after.snapshotName();
            final String repoName = after.repoName();
            return sortOrder == SortOrder.ASC
                ? (info -> compareName(snapshotName, repoName, info) < 0)
                : (info -> compareName(snapshotName, repoName, info) > 0);
        }
    },

    /**
     * Sort by snapshot duration (end time minus start time).
     */
    DURATION("duration", Comparator.comparingLong(sni -> sni.endTime() - sni.startTime())) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return Long.toString(snapshotInfo.endTime() - snapshotInfo.startTime());
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            return after.longValuePredicate(info -> info.endTime() - info.startTime(), sortOrder);
        }
    },

    /**
     * Sort by number of indices in the snapshot.
     */
    INDICES("index_count", Comparator.comparingInt(sni -> sni.indices().size())) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return Integer.toString(snapshotInfo.indices().size());
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            // TODO: cover via pre-flight predicate
            return after.longValuePredicate(info -> info.indices().size(), sortOrder);
        }
    },

    /**
     * Sort by number of shards in the snapshot.
     */
    SHARDS("shard_count", Comparator.comparingInt(SnapshotInfo::totalShards)) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return Integer.toString(snapshotInfo.totalShards());
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            return after.longValuePredicate(SnapshotInfo::totalShards, sortOrder);
        }
    },

    /**
     * Sort by number of failed shards in the snapshot.
     */
    FAILED_SHARDS("failed_shard_count", Comparator.comparingInt(SnapshotInfo::failedShards)) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return Integer.toString(snapshotInfo.failedShards());
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            return after.longValuePredicate(SnapshotInfo::failedShards, sortOrder);
        }
    },

    /**
     * Sort by repository name.
     */
    REPOSITORY("repository", Comparator.comparing(SnapshotInfo::repository)) {
        @Override
        protected String getSortKeyValue(SnapshotInfo snapshotInfo) {
            return snapshotInfo.repository();
        }

        @Override
        protected Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder) {
            // TODO: cover via pre-flight predicate
            final String snapshotName = after.snapshotName();
            final String repoName = after.repoName();
            return sortOrder == SortOrder.ASC
                ? (info -> compareRepositoryName(snapshotName, repoName, info) < 0)
                : (info -> compareRepositoryName(snapshotName, repoName, info) > 0);
        }

        private static int compareRepositoryName(String name, String repoName, SnapshotInfo info) {
            final int res = repoName.compareTo(info.repository());
            if (res != 0) {
                return res;
            }
            return name.compareTo(info.snapshotId().getName());
        }
    };

    private final String name;
    private final Comparator<SnapshotInfo> ascendingSnapshotInfoComparator;
    private final Comparator<SnapshotInfo> descendingSnapshotInfoComparator;

    SnapshotSortKey(String name, Comparator<SnapshotInfo> snapshotInfoComparator) {
        this.name = name;
        this.ascendingSnapshotInfoComparator = snapshotInfoComparator.thenComparing(SnapshotInfo::snapshotId);
        this.descendingSnapshotInfoComparator = ascendingSnapshotInfoComparator.reversed();
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * @return a {@link Comparator} which sorts {@link SnapshotInfo} instances according to this sort key.
     */
    public final Comparator<SnapshotInfo> getSnapshotInfoComparator(SortOrder sortOrder) {
        return switch (sortOrder) {
            case ASC -> ascendingSnapshotInfoComparator;
            case DESC -> descendingSnapshotInfoComparator;
        };
    }

    /**
     * @return an {@link After} which can be included in a {@link GetSnapshotsRequest} (e.g. to be sent to a remote node) and ultimately
     * converted into a predicate to filter out {@link SnapshotInfo} items which were returned on earlier pages of results. See also
     * {@link #encodeAfterQueryParam} and {@link #getAfterPredicate}.
     */
    public static After decodeAfterQueryParam(String param) {
        final String[] parts = new String(Base64.getUrlDecoder().decode(param), StandardCharsets.UTF_8).split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid ?after parameter [" + param + "]");
        }
        return new After(parts[0], parts[1], parts[2]);
    }

    /**
     * @return an encoded representation of the value of the sort key for the given {@link SnapshotInfo}, including the values of the
     * snapshot name and repo name for tiebreaking purposes, which can be returned to the user so they can pass it back to the
     * {@code ?after} param of a subsequent call to the get-snapshots API in order to retrieve the next page of results.
     */
    public final String encodeAfterQueryParam(SnapshotInfo snapshotInfo) {
        final var rawValue = getSortKeyValue(snapshotInfo) + "," + snapshotInfo.repository() + "," + snapshotInfo.snapshotId().getName();
        return Base64.getUrlEncoder().encodeToString(rawValue.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @return a string representation of the value of the sort key for the given {@link SnapshotInfo}, which should be the last item in the
     * response, which is combined with the snapshot and repository names, encoded, and returned to the user so they can pass it back to
     * the {@code ?after} param of a subsequent call to the get-snapshots API in order to retrieve the next page of results.
     */
    protected abstract String getSortKeyValue(SnapshotInfo snapshotInfo);

    /**
     * @return a predicate to filter out {@link SnapshotInfo} items that match the user's query but which sort earlier than the given
     * {@link After} value (i.e. they were returned on earlier pages of results). If {@code after} is {@code null} then the returned
     * predicate matches all snapshots.
     */
    public final Predicate<SnapshotInfo> getAfterPredicate(@Nullable After after, SortOrder sortOrder) {
        return after == null ? Predicates.always() : innerGetAfterPredicate(after, sortOrder);
    }

    /**
     * @return a predicate to filter out {@link SnapshotInfo} items that match the user's query but which sort earlier than the given
     * {@link After} value (i.e. they were returned on earlier pages of results). The {@code after} parameter is not {@code null}.
     */
    protected abstract Predicate<SnapshotInfo> innerGetAfterPredicate(After after, SortOrder sortOrder);

    private static int compareName(String name, String repoName, SnapshotInfo info) {
        final int res = name.compareTo(info.snapshotId().getName());
        if (res != 0) {
            return res;
        }
        return repoName.compareTo(info.repository());
    }

    public static SnapshotSortKey of(String name) {
        return switch (name) {
            case "start_time" -> START_TIME;
            case "name" -> NAME;
            case "duration" -> DURATION;
            case "index_count" -> INDICES;
            case "shard_count" -> SHARDS;
            case "failed_shard_count" -> FAILED_SHARDS;
            case "repository" -> REPOSITORY;
            default -> throw new IllegalArgumentException("unknown sort key [" + name + "]");
        };
    }

    public record After(String value, String repoName, String snapshotName) implements Writeable {

        After(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
            out.writeString(repoName);
            out.writeString(snapshotName);
        }

        Predicate<SnapshotInfo> longValuePredicate(ToLongFunction<SnapshotInfo> extractor, SortOrder sortOrder) {
            final var after = Long.parseLong(value);
            return sortOrder == SortOrder.ASC ? info -> {
                final long val = extractor.applyAsLong(info);
                return after < val || (after == val && compareName(snapshotName, repoName, info) < 0);
            } : info -> {
                final long val = extractor.applyAsLong(info);
                return after > val || (after == val && compareName(snapshotName, repoName, info) > 0);
            };
        }
    }
}
