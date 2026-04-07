/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotInfoCollectorTests extends ESTestCase {

    /**
     * Naive, inefficient, but obviously correct implementation: store all items, sort and possibly limit on demand.
     */
    private static final class NaiveSnapshotInfoCollector implements SnapshotInfoCollector {
        private final List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        private final Comparator<SnapshotInfo> comparator;
        private final int size;
        private final int offset;

        NaiveSnapshotInfoCollector(Comparator<SnapshotInfo> comparator, int size, int offset) {
            this.comparator = comparator;
            this.size = size;
            this.offset = offset;
        }

        @Override
        public void add(SnapshotInfo snapshotInfo) {
            snapshotInfos.add(snapshotInfo);
        }

        @Override
        public List<SnapshotInfo> getSnapshotInfos() {
            snapshotInfos.sort(comparator);
            if (offset >= snapshotInfos.size()) {
                return List.of();
            }
            return snapshotInfos.subList(offset, isLastPage() ? snapshotInfos.size() : offset + size);
        }

        @Override
        public int getRemaining() {
            return isLastPage() ? 0 : snapshotInfos.size() - offset - size;
        }

        private boolean isLastPage() {
            return size == GetSnapshotsRequest.NO_LIMIT
                || offset + size > snapshotInfos.size()
                || (offset + size == snapshotInfos.size() && randomBoolean() /* shouldn't matter if we're right on the boundary */);
        }
    }

    private static SnapshotInfo randomSnapshotInfo() {
        final var startTime = randomNonNegativeLong();
        final var endTime = randomLongBetween(startTime, Long.MAX_VALUE);
        return new SnapshotInfo(
            new Snapshot(ProjectId.DEFAULT, randomRepoName(), new SnapshotId(randomSnapshotName(), randomUUID())),
            randomList(0, 10, ESTestCase::randomIndexName),
            List.of(),
            List.of(),
            null,
            endTime,
            randomInt(),
            Collections.emptyList(),
            null,
            SnapshotInfoTestUtils.randomUserMetadata(),
            startTime,
            SnapshotInfoTestUtils.randomIndexSnapshotDetails()
        );
    }

    public void testMatchesNaiveImplementation() {
        final var comparator = randomSnapshotInfoComparator();
        final var size = randomBoolean() ? GetSnapshotsRequest.NO_LIMIT : between(1, 20);
        final var offset = between(0, 20);
        final var oracle = new NaiveSnapshotInfoCollector(comparator, size, offset);
        final var production = SnapshotInfoCollector.create(comparator, size, offset);

        final var snapshotInfos = randomList(0, 100, SnapshotInfoCollectorTests::randomSnapshotInfo);
        for (SnapshotInfo info : snapshotInfos) {
            oracle.add(info);
        }

        runInParallel(snapshotInfos.size(), i -> production.add(snapshotInfos.get(i)));

        final var expected = oracle.getSnapshotInfos();
        final var actual = production.getSnapshotInfos();
        assertThat(actual.size(), equalTo(expected.size()));
        assertThat(production.getRemaining(), equalTo(oracle.getRemaining()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("value at " + i, actual.get(i), sameInstance(expected.get(i)));
        }
    }

    public void testOverflow() {
        final var offset = between(1, Integer.MAX_VALUE - 1);
        final var size = Integer.MAX_VALUE - between(0, offset - 1);
        expectThrows(IllegalArgumentException.class, () -> SnapshotInfoCollector.create(randomSnapshotInfoComparator(), size, offset));
    }

    private static Comparator<SnapshotInfo> randomSnapshotInfoComparator() {
        return randomFrom(SnapshotSortKey.values()).getSnapshotInfoComparator(randomFrom(SortOrder.values()));
    }
}
