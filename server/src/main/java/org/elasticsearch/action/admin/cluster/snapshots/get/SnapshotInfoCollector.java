/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Strategy for collecting {@link SnapshotInfo} results, either retaining all (unbounded) or only the top N by sort order (bounded).
 */
interface SnapshotInfoCollector {

    void add(SnapshotInfo snapshotInfo);

    /**
     * Returns how many snapshots remain after the current page.
     */
    int getRemaining();

    /**
     * Returns the requested page of {@link SnapshotInfo} instances.
     * <p>
     * Caller must not mutate the returned list.
     */
    List<SnapshotInfo> getSnapshotInfos();

    /**
     * Creates a {@link SnapshotInfoCollector} suitable for collecting the specified page of results.
     */
    static SnapshotInfoCollector create(Comparator<SnapshotInfo> comparator, int size, int offset) {
        assert size == GetSnapshotsRequest.NO_LIMIT || size > 0 : "size must be NO_LIMIT or positive";
        assert offset >= 0 : "offset must be non-negative";
        return size == GetSnapshotsRequest.NO_LIMIT
            ? new UnboundedSnapshotInfoCollector(comparator, offset)
            : new BoundedSnapshotInfoCollector(comparator, offset, size);
    }

    /**
     * Retains all snapshots and sorts when building the result list. Only used when size is
     * {@link GetSnapshotsRequest#NO_LIMIT}, so the page is always from offset to the end.
     */
    final class UnboundedSnapshotInfoCollector implements SnapshotInfoCollector {
        private final List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        private final Comparator<SnapshotInfo> comparator;
        private final int offset;

        UnboundedSnapshotInfoCollector(Comparator<SnapshotInfo> comparator, int offset) {
            this.comparator = comparator;
            this.offset = offset;
        }

        @Override
        public synchronized void add(SnapshotInfo snapshotInfo) {
            snapshotInfos.add(snapshotInfo);
        }

        @Override
        public int getRemaining() {
            return 0;
        }

        @Override
        public List<SnapshotInfo> getSnapshotInfos() {
            // No synchronization needed: the caller invokes this only after all add() calls have completed.
            if (offset >= snapshotInfos.size()) {
                return List.of();
            }
            snapshotInfos.sort(comparator);
            return snapshotInfos.subList(offset, snapshotInfos.size());
        }
    }

    /**
     * Retains only the top (capacity) snapshots by sort order to bound memory use.
     */
    final class BoundedSnapshotInfoCollector implements SnapshotInfoCollector {
        private final PriorityQueue<SnapshotInfo> snapshotInfos;
        private final int capacity;
        private final Comparator<SnapshotInfo> comparator;
        private final int offset;
        private int collectedCount;

        BoundedSnapshotInfoCollector(Comparator<SnapshotInfo> comparator, int offset, int size) {
            assert size > 0;
            this.capacity = offset + size;
            this.snapshotInfos = new PriorityQueue<>(capacity, comparator.reversed()); // throws IAE if size+offset overflowed
            this.comparator = comparator;
            this.offset = offset;
        }

        @Override
        public synchronized void add(SnapshotInfo snapshotInfo) {
            incrementCollectedCount();
            if (snapshotInfos.size() < capacity) {
                snapshotInfos.add(snapshotInfo);
            } else {
                final var worst = snapshotInfos.peek();
                if (comparator.compare(snapshotInfo, worst) < 0) {
                    snapshotInfos.poll();
                    snapshotInfos.add(snapshotInfo);
                }
            }
        }

        private void incrementCollectedCount() {
            if (collectedCount == Integer.MAX_VALUE) {
                throw new ElasticsearchStatusException(
                    "cannot process more than [{}] snapshots",
                    RestStatus.TOO_MANY_REQUESTS,
                    Integer.MAX_VALUE
                );
            }
            collectedCount += 1;
        }

        @Override
        public int getRemaining() {
            return Math.max(0, collectedCount - capacity);
        }

        @Override
        public List<SnapshotInfo> getSnapshotInfos() {
            // No synchronization needed: the caller invokes this only after all add() calls have completed.
            if (offset >= snapshotInfos.size()) {
                return List.of();
            }
            final List<SnapshotInfo> drained = new ArrayList<>(snapshotInfos);
            drained.sort(comparator); // PriorityQueue's iterator returns elements in heap order, not sorted order, so we must sort.
            return drained.subList(offset, drained.size());
        }
    }
}
