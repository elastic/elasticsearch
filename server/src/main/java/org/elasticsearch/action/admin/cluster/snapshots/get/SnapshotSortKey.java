/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.Comparator;

/**
 * Sort key for snapshots e.g. returned from the get-snapshots API. All values break ties using {@link SnapshotInfo#snapshotId} (i.e. by
 * name).
 */
public enum SnapshotSortKey {
    /**
     * Sort by snapshot start time.
     */
    START_TIME("start_time", Comparator.comparingLong(SnapshotInfo::startTime)),

    /**
     * Sort by snapshot name.
     */
    NAME("name", Comparator.comparing(sni -> sni.snapshotId().getName())),

    /**
     * Sort by snapshot duration (end time minus start time).
     */
    DURATION("duration", Comparator.comparingLong(sni -> sni.endTime() - sni.startTime())),
    /**
     * Sort by number of indices in the snapshot.
     */
    INDICES("index_count", Comparator.comparingInt(sni -> sni.indices().size())),

    /**
     * Sort by number of shards in the snapshot.
     */
    SHARDS("shard_count", Comparator.comparingInt(SnapshotInfo::totalShards)),

    /**
     * Sort by number of failed shards in the snapshot.
     */
    FAILED_SHARDS("failed_shard_count", Comparator.comparingInt(SnapshotInfo::failedShards)),

    /**
     * Sort by repository name.
     */
    REPOSITORY("repository", Comparator.comparing(SnapshotInfo::repository));

    private final String name;
    private final Comparator<SnapshotInfo> snapshotInfoComparator;

    SnapshotSortKey(String name, Comparator<SnapshotInfo> snapshotInfoComparator) {
        this.name = name;
        this.snapshotInfoComparator = snapshotInfoComparator.thenComparing(SnapshotInfo::snapshotId);
    }

    @Override
    public String toString() {
        return name;
    }

    public final Comparator<SnapshotInfo> getSnapshotInfoComparator() {
        return snapshotInfoComparator;
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
}
