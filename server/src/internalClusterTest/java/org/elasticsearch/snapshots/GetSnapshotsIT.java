/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        createRepository(repoName, "fs");
        final List<String> snapshotNamesWithoutIndex = createNSnapshots(repoName, randomIntBetween(3, 20));

        createIndexWithContent("test-index");

        final List<String> snapshotNamesWithIndex = createNSnapshots(repoName, randomIntBetween(3, 20));

        final Collection<String> allSnapshotNames = new HashSet<>(snapshotNamesWithIndex);
        allSnapshotNames.addAll(snapshotNamesWithoutIndex);

        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(repoName);
        assertSorted(defaultSorting, (s1, s2) -> assertThat(s2, greaterThanOrEqualTo(s1)));
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.NAME),
            (s1, s2) -> assertThat(s2.snapshotId().getName(), greaterThanOrEqualTo(s1.snapshotId().getName()))
        );
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.DURATION),
            (s1, s2) -> assertThat(s2.endTime() - s2.startTime(), greaterThanOrEqualTo(s1.endTime() - s1.startTime()))
        );
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.INDICES),
            (s1, s2) -> assertThat(s2.indices().size(), greaterThanOrEqualTo(s1.indices().size()))
        );
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.START_TIME),
            (s1, s2) -> assertThat(s2.startTime(), greaterThanOrEqualTo(s1.startTime()))
        );
    }

    private List<SnapshotInfo> allSnapshotsSorted(Collection<String> allSnapshotNames, String repoName, GetSnapshotsAction.SortBy sortBy) {
        final List<SnapshotInfo> snapshotInfos = clusterAdmin()
            .prepareGetSnapshots(repoName)
            .sortBy(sortBy)
            .get()
            .getSnapshots(repoName);
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private static void assertSorted(List<SnapshotInfo> snapshotInfos, BiConsumer<SnapshotInfo, SnapshotInfo> assertion) {
        for (int i = 0; i < snapshotInfos.size() - 1; i++) {
            assertion.accept(snapshotInfos.get(i), snapshotInfos.get(i + 1));
        }
    }
}
