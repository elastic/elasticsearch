/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GetSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    public void testSortOrder() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final List<String> snapshotNamesWithoutIndex = createNSnapshots(repoName, randomIntBetween(3, 20));

        createIndexWithContent("test-index");

        final List<String> snapshotNamesWithIndex = createNSnapshots(repoName, randomIntBetween(3, 20));

        final Collection<String> allSnapshotNames = new HashSet<>(snapshotNamesWithIndex);
        allSnapshotNames.addAll(snapshotNamesWithoutIndex);

        final List<SnapshotInfo> defaultSorting = baseGetSnapshotsRequest(repoName).get().getSnapshots(repoName);
        assertSnapshotListSorted(defaultSorting, null);
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.NAME), GetSnapshotsAction.SortBy.NAME);
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.DURATION), GetSnapshotsAction.SortBy.DURATION
        );
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.INDICES), GetSnapshotsAction.SortBy.INDICES);
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.START_TIME), GetSnapshotsAction.SortBy.START_TIME
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final List<String> names = createNSnapshots(repoName, randomIntBetween(6, 20));
        for (GetSnapshotsAction.SortBy sort : GetSnapshotsAction.SortBy.values()) {
            logger.info("--> testing pagination for [{}]", sort);
            doTestResponseSizeLimit(sort, repoName, names);
        }
    }

    public void testSortAndPaginateWithInProgress() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final Collection<String> allSnapshotNames = new HashSet<>(createNSnapshots(repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-1");
        allSnapshotNames.addAll(createNSnapshots(repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-2");

        final int inProgressCount = randomIntBetween(6, 20);
        final List<ActionFuture<CreateSnapshotResponse>> inProgressSnapshots = new ArrayList<>(inProgressCount);
        blockAllDataNodes(repoName);
        for (int i = 0; i < inProgressCount; i++) {
            final String snapshotName = "snap-" + i;
            allSnapshotNames.add(snapshotName);
            inProgressSnapshots.add(startFullSnapshot(repoName, snapshotName));
        }
        awaitNumberOfSnapshotsInProgress(inProgressCount);

        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.NAME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.INDICES);

        unblockAllDataNodes(repoName);
        for (ActionFuture<CreateSnapshotResponse> inProgressSnapshot : inProgressSnapshots) {
            assertSuccessful(inProgressSnapshot);
        }

        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.NAME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsAction.SortBy.INDICES);
    }

    private static void assertStablePagination(String repoName, Collection<String> allSnapshotNames, GetSnapshotsAction.SortBy sort) {
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, repoName, sort);

        for (int i = 1; i <= allSnapshotNames.size(); i++) {
            final List<SnapshotInfo> subsetSorted = sortedWithLimit(repoName, sort, null, i);
            assertEquals(subsetSorted, allSorted.subList(0, i));
        }

        for (int j = 0; j < allSnapshotNames.size(); j++) {
            final SnapshotInfo after = allSorted.get(j);
            for (int i = 1; i < allSnapshotNames.size() - j; i++) {
                final List<SnapshotInfo> subsetSorted = sortedWithLimit(repoName, sort, after, i);
                assertEquals(subsetSorted, allSorted.subList(j + 1, j + i + 1));
            }
        }
    }

    private static void doTestResponseSizeLimit(GetSnapshotsAction.SortBy sort, String repoName, List<String> snapshotNames) {
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(snapshotNames, repoName, sort);
        final List<SnapshotInfo> batch1 = sortedWithLimit(repoName, sort, null, 2);
        assertEquals(batch1, allSnapshotsSorted.subList(0, 2));
        final List<SnapshotInfo> batch2 = sortedWithLimit(repoName, sort, batch1.get(1), 2);
        assertEquals(batch2, allSnapshotsSorted.subList(2, 4));
        final int lastBatch = snapshotNames.size() - batch1.size() - batch2.size();
        final List<SnapshotInfo> batch3 = sortedWithLimit(repoName, sort, batch2.get(1), lastBatch);
        assertEquals(batch3, allSnapshotsSorted.subList(batch1.size() + batch2.size(), snapshotNames.size()));
    }

    private static List<SnapshotInfo> allSnapshotsSorted(Collection<String> allSnapshotNames,
                                                         String repoName,
                                                         GetSnapshotsAction.SortBy sortBy) {
        final List<SnapshotInfo> snapshotInfos = sortedWithLimit(repoName, sortBy, null, 0);
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private static List<SnapshotInfo> sortedWithLimit(String repoName, GetSnapshotsAction.SortBy sortBy, SnapshotInfo after, int size) {
        final List<SnapshotInfo> snapshotInfos = baseGetSnapshotsRequest(repoName)
                .pagination(after, sortBy, size)
                .get()
                .getSnapshots(repoName);
        if (size > 0) {
            assertThat(snapshotInfos, hasSize(size));
        }
        return snapshotInfos;
    }

    private static GetSnapshotsRequestBuilder baseGetSnapshotsRequest(String repoName) {
        final GetSnapshotsRequestBuilder builder = clusterAdmin().prepareGetSnapshots(repoName);
        // exclude old version snapshot from test assertions every time and do a prefixed query in either case half the time
        if (randomBoolean() ||
                clusterAdmin().prepareGetSnapshots(repoName)
                        .setSnapshots(AbstractSnapshotIntegTestCase.OLD_VERSION_SNAPSHOT_PREFIX + "*")
                        .setIgnoreUnavailable(true)
                        .get()
                        .getSnapshots(repoName)
                        .isEmpty() == false
        ) {
            builder.setSnapshots(RANDOM_SNAPSHOT_NAME_PREFIX + "*");
        }
        return builder;
    }
}
