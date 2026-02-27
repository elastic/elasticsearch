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

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.action.admin.cluster.snapshots.get.After.fromSnapshotInfo;
import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.EXCLUDE;
import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.INCLUDE;
import static org.elasticsearch.action.admin.cluster.snapshots.get.PreflightFilterResult.INCONCLUSIVE;
import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.createRandomSnapshotInfo;

public class FromSortValuePredicatesTests extends ESTestCase {

    // exclude SHARDS and FAILED_SHARDS in the preflight cases since these require SnapshotInfo to be loaded
    private static final EnumSet<SnapshotSortKey> PREFLIGHT_TEST_KEYS = EnumSet.of(
        SnapshotSortKey.NAME,
        SnapshotSortKey.START_TIME,
        SnapshotSortKey.INDICES,
        SnapshotSortKey.DURATION
    );

    // preflight cases which might or might not work depending on whether SnapshotDetails is present
    private static final EnumSet<SnapshotSortKey> PREFLIGHT_UNRELIABLE_TEST_KEYS = EnumSet.of(
        SnapshotSortKey.START_TIME,
        SnapshotSortKey.DURATION
    );

    // SHARDS and FAILED_SHARDS always require SnapshotInfo to be loaded
    private static final EnumSet<SnapshotSortKey> PREFLIGHT_INCONCLUSIVE_TEST_KEYS = EnumSet.of(
        SnapshotSortKey.SHARDS,
        SnapshotSortKey.FAILED_SHARDS
    );

    // exclude NAME/REPOSITORY/INDICES in the SnapshotInfo cases since these are reliably handled by the pre-flight test
    private static final EnumSet<SnapshotSortKey> SNAPSHOT_INFO_TEST_KEYS = EnumSet.of(
        SnapshotSortKey.START_TIME,
        SnapshotSortKey.DURATION,
        SnapshotSortKey.SHARDS,
        SnapshotSortKey.FAILED_SHARDS
    );

    private final SnapshotSortKey sortBy;
    private final SortOrder order;

    private SnapshotInfo info1;
    private SnapshotInfo info2;

    private String sortValue1;
    private String sortValue2;

    private RepositoryData repositoryData1NoDetails;
    private RepositoryData repositoryData2NoDetails;

    private RepositoryData repositoryData1;
    private RepositoryData repositoryData2;

    public FromSortValuePredicatesTests(SnapshotSortKey sortBy, SortOrder order) {
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
        sortValue1 = fromSnapshotInfo(info1, sortBy).value();
        sortValue2 = fromSnapshotInfo(info2, sortBy).value();
        repositoryData1NoDetails = buildRepositoryDataNoDetails(info1);
        repositoryData2NoDetails = buildRepositoryDataNoDetails(info2);
        repositoryData1 = repositoryData1NoDetails.withExtraDetails(
            Map.of(info1.snapshotId(), RepositoryData.SnapshotDetails.fromSnapshotInfo(info1))
        );
        repositoryData2 = repositoryData2NoDetails.withExtraDetails(
            Map.of(info2.snapshotId(), RepositoryData.SnapshotDetails.fromSnapshotInfo(info2))
        );
    }

    private RepositoryData buildRepositoryDataNoDetails(SnapshotInfo snapshotInfo) {
        return new RepositoryData(
            randomUUID(),
            randomNonNegativeLong(),
            Map.of(snapshotInfo.snapshotId().getUUID(), snapshotInfo.snapshotId()),
            randomFrom(Map.of(), Map.of(snapshotInfo.snapshotId().getUUID(), RepositoryData.SnapshotDetails.EMPTY)),
            IntStream.range(0, snapshotInfo.indices().size())
                .boxed()
                .collect(Collectors.toMap(i -> new IndexId("idx-" + i, UUIDs.randomBase64UUID()), i -> List.of(snapshotInfo.snapshotId()))),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            randomUUID()
        );
    }

    private FromSortValuePredicates getPredicates(String fromSortValue) {
        return FromSortValuePredicates.forFromSortValue(fromSortValue, sortBy, order);
    }

    public void testSnapshotInfoPredicate() {
        assertTrue(getPredicates(sortValue1).test(info1));
        assertTrue(getPredicates(sortValue1).test(info2));
        assertEquals(SNAPSHOT_INFO_TEST_KEYS.contains(sortBy) == false, getPredicates(sortValue2).test(info1));
        assertTrue(getPredicates(sortValue2).test(info2));
    }

    public void testPreflightConclusive() {
        if (PREFLIGHT_TEST_KEYS.contains(sortBy)) {
            assertEquals(INCLUDE, getPredicates(sortValue1).test(info1.snapshotId(), repositoryData1));
            assertEquals(INCLUDE, getPredicates(sortValue1).test(info2.snapshotId(), repositoryData2));
            assertEquals(EXCLUDE, getPredicates(sortValue2).test(info1.snapshotId(), repositoryData1));
            assertEquals(INCLUDE, getPredicates(sortValue2).test(info2.snapshotId(), repositoryData2));

            if (PREFLIGHT_UNRELIABLE_TEST_KEYS.contains(sortBy) == false) {
                assertEquals(INCLUDE, getPredicates(sortValue1).test(info1.snapshotId(), repositoryData1NoDetails));
                assertEquals(INCLUDE, getPredicates(sortValue1).test(info2.snapshotId(), repositoryData2NoDetails));
                assertEquals(EXCLUDE, getPredicates(sortValue2).test(info1.snapshotId(), repositoryData1NoDetails));
                assertEquals(INCLUDE, getPredicates(sortValue2).test(info2.snapshotId(), repositoryData2NoDetails));
            }
        }
    }

    public void testPreflightInconclusive() {
        if (PREFLIGHT_UNRELIABLE_TEST_KEYS.contains(sortBy) || PREFLIGHT_INCONCLUSIVE_TEST_KEYS.contains(sortBy)) {
            assertEquals(INCONCLUSIVE, getPredicates(sortValue1).test(info1.snapshotId(), repositoryData1NoDetails));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue1).test(info2.snapshotId(), repositoryData2NoDetails));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue2).test(info1.snapshotId(), repositoryData1NoDetails));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue2).test(info2.snapshotId(), repositoryData2NoDetails));
        }

        if (PREFLIGHT_INCONCLUSIVE_TEST_KEYS.contains(sortBy)) {
            assertEquals(INCONCLUSIVE, getPredicates(sortValue1).test(info1.snapshotId(), repositoryData1));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue1).test(info2.snapshotId(), repositoryData2));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue2).test(info1.snapshotId(), repositoryData1));
            assertEquals(INCONCLUSIVE, getPredicates(sortValue2).test(info2.snapshotId(), repositoryData2));
        }
    }

    public void testPreflightMatchAllWhenNullOrRepositorySort() {
        final var predicates = getPredicates(
            sortBy == SnapshotSortKey.REPOSITORY ? randomFrom(sortValue1, sortValue2, randomRepoName()) : null
        );
        assertEquals(INCLUDE, predicates.test(info1.snapshotId(), randomFrom(repositoryData1, repositoryData1NoDetails)));
        assertEquals(INCLUDE, predicates.test(info2.snapshotId(), randomFrom(repositoryData2, repositoryData2NoDetails)));
        assertTrue(predicates.test(info1));
        assertTrue(predicates.test(info2));
    }
}
