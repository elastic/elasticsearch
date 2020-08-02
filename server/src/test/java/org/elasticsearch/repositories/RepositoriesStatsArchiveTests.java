/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RepositoriesStatsArchiveTests extends ESTestCase {
    public void testStatsAreEvictedOnceTheyAreOlderThanRetentionPeriod() {
        int retentionTimeInHours = randomIntBetween(1, 4);
        RepositoriesStatsArchive repositoriesStatsArchive =
            new RepositoriesStatsArchive(TimeValue.timeValueHours(retentionTimeInHours));

        int statsOlderThanRetentionPeriodCount = randomInt(10);
        for (int i = 0; i < statsOlderThanRetentionPeriodCount; i++) {
            RepositoryInfo repositoryInfo =
                createRepositoryInfo(retentionTimeInHours + 2, retentionTimeInHours + 1);

            RepositoryStatsSnapshot repoStats =
                new RepositoryStatsSnapshot(repositoryInfo, RepositoryStats.EMPTY_STATS);

            repositoriesStatsArchive.archive(repoStats);
        }

        int statsToBeRetainedCount = randomInt(10);
        for (int i = 0; i < statsToBeRetainedCount; i++) {
            RepositoryInfo repositoryInfo =
                createRepositoryInfo(0);

            RepositoryStatsSnapshot repoStats =
                new RepositoryStatsSnapshot(repositoryInfo, new RepositoryStats(Map.of("GET", 10L)));
            repositoriesStatsArchive.archive(repoStats);
        }

        List<RepositoryStatsSnapshot> archivedStats = repositoriesStatsArchive.getArchivedStats();
        assertThat(archivedStats.size(), equalTo(statsToBeRetainedCount));
        for (RepositoryStatsSnapshot repositoryStatsSnapshot : archivedStats) {
            assertThat(repositoryStatsSnapshot.getRepositoryStats().requestCounts, equalTo(Map.of("GET", 10L)));
        }
    }

    private RepositoryInfo createRepositoryInfo(int hoursSinceRepoStarted) {
        return new RepositoryInfo(UUIDs.randomBase64UUID(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Instant.now().minus(Duration.ofHours(hoursSinceRepoStarted)),
            null);
    }

    private RepositoryInfo createRepositoryInfo(int hoursSinceRepoStarted, int hoursSinceRepoStopped) {
        assert hoursSinceRepoStarted > hoursSinceRepoStopped;
        return new RepositoryInfo(UUIDs.randomBase64UUID(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Instant.now().minus(Duration.ofHours(hoursSinceRepoStarted)),
            Instant.now().minus(Duration.ofHours(hoursSinceRepoStopped)));
    }
}
