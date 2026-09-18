/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * For every group of a {@link TimeSeriesBlockHash}, the previous and the next <em>populated</em> group of the same tsid,
 * i.e. the groups with the closest smaller and the closest larger timestamp. Unlike a lookup of the directly adjacent
 * time bucket, this skips over empty buckets, so a group whose neighbouring buckets received no samples still links
 * to the nearest bucket that did.
 * <p>
 * The computation runs in two phases:
 * <ol>
 *     <li>Cluster the groups by tsid in {@code O(numGroups)} using a counting sort over the dense tsid ordinals of the
 *     hash: count the groups per tsid, turn the counts into exclusive prefix sums (the write offset of each tsid), then
 *     scatter every group into a single array at its tsid's write offset.</li>
 *     <li>Walk the clustered array one tsid at a time. The groups of a tsid are sorted by timestamp in descending order
 *     into a reusable scratch array sized for the largest tsid, and consecutive entries are linked as previous/next.
 *     Descending order is chosen because time-series sources emit the samples of a tsid newest first, so the groups
 *     are usually already (nearly) sorted that way and TimSort degrades to a linear scan.</li>
 * </ol>
 */
final class TimeSeriesAdjacentGroups implements Releasable {

    /**
     * A group and the timestamp of its time bucket, ordered by timestamp descending. Instances are mutable so a single
     * scratch array can be reused across tsids without re-allocating.
     */
    static final class GroupWithTimestamp implements Comparable<GroupWithTimestamp> {
        int groupId;
        long timestamp;

        @Override
        public int compareTo(GroupWithTimestamp other) {
            return Long.compare(other.timestamp, timestamp);
        }
    }

    private final IntArray prevGroupIds;
    private final IntArray nextGroupIds;

    private TimeSeriesAdjacentGroups(IntArray prevGroupIds, IntArray nextGroupIds) {
        this.prevGroupIds = prevGroupIds;
        this.nextGroupIds = nextGroupIds;
    }

    /**
     * Computes the adjacent groups for every group currently in {@code blockHash}.
     */
    static TimeSeriesAdjacentGroups compute(TimeSeriesBlockHash blockHash, BigArrays bigArrays) {
        final long numGroups = blockHash.numGroups();
        IntArray prevGroupIds = null;
        IntArray nextGroupIds = null;
        boolean success = false;
        try {
            prevGroupIds = bigArrays.newIntArray(numGroups, false);
            prevGroupIds.fill(0, numGroups, -1);
            nextGroupIds = bigArrays.newIntArray(numGroups, false);
            nextGroupIds.fill(0, numGroups, -1);
            if (numGroups > 0) {
                link(blockHash, bigArrays, prevGroupIds, nextGroupIds);
            }
            success = true;
            return new TimeSeriesAdjacentGroups(prevGroupIds, nextGroupIds);
        } finally {
            if (success == false) {
                Releasables.close(prevGroupIds, nextGroupIds);
            }
        }
    }

    private static void link(TimeSeriesBlockHash blockHash, BigArrays bigArrays, IntArray prevGroupIds, IntArray nextGroupIds) {
        final long numGroups = blockHash.numGroups();
        final int numTsids = blockHash.numTsids();
        try (
            IntArray groupsPerTsid = bigArrays.newIntArray(numTsids, true);
            IntArray clusteredGroups = bigArrays.newIntArray(numGroups, false)
        ) {
            // Phase 1a: count the groups of each tsid, remembering the largest count to size the scratch array.
            int maxGroupsPerTsid = 0;
            for (long groupId = 0; groupId < numGroups; groupId++) {
                groupsPerTsid.increment(blockHash.tsidForGroup(groupId), 1);
            }
            // Phase 1b: exclusive prefix sums of the counts are the write offsets; scatter the groups by tsid.
            try (IntArray writeOffsets = bigArrays.newIntArray(numTsids, false)) {
                int offset = 0;
                for (int tsid = 0; tsid < numTsids; tsid++) {
                    writeOffsets.set(tsid, offset);
                    int numGroupsForTsid = groupsPerTsid.get(tsid);
                    offset += numGroupsForTsid;
                    maxGroupsPerTsid = Math.max(maxGroupsPerTsid, numGroupsForTsid);
                }
                assert offset == numGroups : "expected " + numGroups + " groups but counted " + offset;
                for (long groupId = 0; groupId < numGroups; groupId++) {
                    int position = writeOffsets.increment(blockHash.tsidForGroup(groupId), 1) - 1;
                    clusteredGroups.set(position, Math.toIntExact(groupId));
                }
            }
            // Phase 2: sort the groups of each tsid by timestamp (descending) and link consecutive ones.
            final GroupWithTimestamp[] sorted = new GroupWithTimestamp[maxGroupsPerTsid];
            for (int i = 0; i < sorted.length; i++) {
                sorted[i] = new GroupWithTimestamp();
            }
            long start = 0;
            while (start < numGroups) {
                final int tsid = blockHash.tsidForGroup(clusteredGroups.get(start));
                final int count = groupsPerTsid.get(tsid);
                for (int i = 0; i < count; i++) {
                    int groupId = clusteredGroups.get(start + i);
                    sorted[i].groupId = groupId;
                    sorted[i].timestamp = blockHash.timestampForGroup(groupId);
                }
                // Arrays.sort on an object array is TimSort, which is linear on (nearly) sorted input.
                Arrays.sort(sorted, 0, count);
                for (int i = 1; i < count; i++) {
                    int newerGroupId = sorted[i - 1].groupId;
                    int olderGroupId = sorted[i].groupId;
                    assert sorted[i - 1].timestamp > sorted[i].timestamp
                        : "groups "
                            + newerGroupId
                            + " and "
                            + olderGroupId
                            + " of tsid "
                            + tsid
                            + " share timestamp "
                            + sorted[i].timestamp;
                    nextGroupIds.set(olderGroupId, newerGroupId);
                    prevGroupIds.set(newerGroupId, olderGroupId);
                }
                start += count;
            }
        }
    }

    /**
     * The group of the same tsid with the closest smaller timestamp, or {@code -1} if {@code groupId} is the oldest.
     */
    int previousGroupId(int groupId) {
        return prevGroupIds.get(groupId);
    }

    /**
     * The group of the same tsid with the closest larger timestamp, or {@code -1} if {@code groupId} is the newest.
     */
    int nextGroupId(int groupId) {
        return nextGroupIds.get(groupId);
    }

    @Override
    public void close() {
        Releasables.close(prevGroupIds, nextGroupIds);
    }
}
