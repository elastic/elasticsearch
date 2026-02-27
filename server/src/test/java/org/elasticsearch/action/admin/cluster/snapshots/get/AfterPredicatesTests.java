/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;

import static org.elasticsearch.action.admin.cluster.snapshots.get.After.fromSnapshotInfo;
import static org.elasticsearch.search.sort.SortOrder.DESC;
import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.createRandomSnapshotInfo;

public class AfterPredicatesTests extends ESTestCase {

    private final SnapshotSortKey sortBy;
    private final SortOrder order;

    private SnapshotInfo info1;
    private SnapshotInfo info2;

    private After after1;
    private After after2;

    public AfterPredicatesTests(SnapshotSortKey sortBy, SortOrder order) {
        this.sortBy = sortBy;
        this.order = order;
    }

    @ParametersFactory(argumentFormatting = "sortBy=%s order=%s")
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(SnapshotSortKey.values())
            .flatMap(k -> Arrays.stream(SortOrder.values()).map(o -> new Object[] { k, o }))
            .toList();
    }

    @Before
    public void setUpSnapshotInfos() {
        final var infoA = createRandomSnapshotInfo();
        final var infoB = randomValueOtherThanMany(
            info -> infoA.startTime() == info.startTime()
                || infoA.snapshotId().getName().equals(info.snapshotId().getName())
                || infoA.endTime() - infoA.startTime() == info.endTime() - info.startTime()
                || infoA.indices().size() == info.indices().size()
                || infoA.totalShards() == info.totalShards()
                || infoA.failedShards() == info.failedShards()
                || infoA.repository().equals(info.repository()),
            SnapshotInfoTestUtils::createRandomSnapshotInfo
        );
        final int comparison = sortBy.getSnapshotInfoComparator(order).compare(infoA, infoB);
        assertNotEquals(0, comparison);
        info1 = comparison < 0 ? infoA : infoB;
        info2 = comparison < 0 ? infoB : infoA;
        after1 = fromSnapshotInfo(info1, sortBy);
        after2 = fromSnapshotInfo(info2, sortBy);
    }

    private void verifyPredicates(After after, boolean expectInfo1Included, boolean expectInfo2Included) {
        final var predicates = AfterPredicates.forAfter(after, sortBy, order);
        assertEquals(expectInfo1Included, predicates.test(info1));
        assertEquals(expectInfo2Included, predicates.test(info2));
    }

    public void testMatchAll() {
        verifyPredicates(null, true, true);
    }

    public void testAfterFirst() {
        verifyPredicates(after1, false, true);
    }

    public void testAfterSecond() {
        verifyPredicates(after2, false, false);
    }

    public void testSnapshotNameTiebreak() {
        if (sortBy == SnapshotSortKey.NAME) {
            return;
        }

        verifyPredicates(
            new After(
                after1.value(),
                sortBy == SnapshotSortKey.REPOSITORY ? after1.repoName() : randomRepoName(),
                stringBefore(after1.snapshotName())
            ),
            true,
            true
        );
    }

    public void testRepositoryNameTiebreak() {
        if (sortBy == SnapshotSortKey.REPOSITORY) {
            return;
        }

        verifyPredicates(new After(after1.value(), stringBefore(after1.repoName()), after1.snapshotName()), true, true);
    }

    private String stringBefore(String candidate) {
        return order == DESC ? candidate + "z" : "";
    }
}
