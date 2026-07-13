/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class RepositoriesStatsArchiveTests extends ESTestCase {
    public void testStatsAreEvictedOnceTheyAreOlderThanRetentionPeriod() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);

        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive = new RepositoriesStatsArchive(
            TimeValue.timeValueMillis(retentionTimeInMillis),
            100,
            fakeRelativeClock::get
        );

        for (int i = 0; i < randomInt(10); i++) {
            RepositoryStatsSnapshot repoStats = createRepositoryStats(RepositoryStats.EMPTY_STATS);
            repositoriesStatsArchive.archive(repoStats);
        }

        fakeRelativeClock.set(retentionTimeInMillis * 2);
        int statsToBeRetainedCount = randomInt(10);
        for (int i = 0; i < statsToBeRetainedCount; i++) {
            RepositoryStatsSnapshot repoStats = createRepositoryStats(new RepositoryStats(Map.of("GET", new BlobStoreActionStats(10, 13))));
            repositoriesStatsArchive.archive(repoStats);
        }

        List<RepositoryStatsSnapshot> archivedStats = repositoriesStatsArchive.getArchivedStats();
        assertThat(archivedStats.size(), equalTo(statsToBeRetainedCount));
        for (RepositoryStatsSnapshot repositoryStatsSnapshot : archivedStats) {
            assertThat(repositoryStatsSnapshot.getRepositoryStats().actionStats, equalTo(Map.of("GET", new BlobStoreActionStats(10, 13))));
        }
    }

    public void testStatsAreRejectedIfTheArchiveIsFull() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);

        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive = new RepositoriesStatsArchive(
            TimeValue.timeValueMillis(retentionTimeInMillis),
            1,
            fakeRelativeClock::get
        );

        assertTrue(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));

        fakeRelativeClock.set(retentionTimeInMillis * 2);
        // Now there's room since the previous stats should be evicted
        assertTrue(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));
        // There's no room for stats with the same creation time
        assertFalse(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));
    }

    public void testClearArchive() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);
        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive = new RepositoriesStatsArchive(
            TimeValue.timeValueMillis(retentionTimeInMillis),
            100,
            fakeRelativeClock::get
        );

        int archivedStatsWithVersionZero = randomIntBetween(1, 20);
        for (int i = 0; i < archivedStatsWithVersionZero; i++) {
            repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS, 0));
        }

        int archivedStatsWithNewerVersion = randomIntBetween(1, 20);
        for (int i = 0; i < archivedStatsWithNewerVersion; i++) {
            repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS, 1));
        }

        List<RepositoryStatsSnapshot> removedStats = repositoriesStatsArchive.clear(0L);
        assertThat(removedStats.size(), equalTo(archivedStatsWithVersionZero));

        assertThat(repositoriesStatsArchive.getArchivedStats().size(), equalTo(archivedStatsWithNewerVersion));
    }

    private RepositoryStatsSnapshot createRepositoryStats(RepositoryStats repositoryStats) {
        return createRepositoryStats(repositoryStats, 0);
    }

    private RepositoryStatsSnapshot createRepositoryStats(RepositoryStats repositoryStats, long clusterVersion) {
        RepositoryInfo repositoryInfo = new RepositoryInfo(
            UUIDs.randomBase64UUID(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Map.of("bucket", randomAlphaOfLength(10)),
            System.currentTimeMillis(),
            null
        );
        return new RepositoryStatsSnapshot(repositoryInfo, repositoryStats, clusterVersion, true);
    }

}
