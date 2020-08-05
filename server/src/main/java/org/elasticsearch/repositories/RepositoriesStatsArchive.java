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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.LongSupplier;

public final class RepositoriesStatsArchive {
    private static final Logger logger = LogManager.getLogger(RepositoriesStatsArchive.class);

    private final TimeValue retentionPeriod;
    private final int maxCapacity;
    private final LongSupplier relativeTimeSupplier;
    private final LongSupplier absoluteTimeSupplier;
    private final Deque<RepositoryStatsSnapshot> archive = new ArrayDeque<>();

    public RepositoriesStatsArchive(TimeValue retentionPeriod,
                                    int maxCapacity,
                                    LongSupplier relativeTimeSupplier,
                                    LongSupplier absoluteTimeSupplier) {
        this.retentionPeriod = retentionPeriod;
        this.maxCapacity = maxCapacity;
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.absoluteTimeSupplier = absoluteTimeSupplier;
    }

    /**
     * Archives the specified repository stats snapshot into the archive
     * if it's possible without violating the capacity constraints.
     *
     * @return {@code true} if the repository stats were archived, {@code false} otherwise.
     */
    synchronized boolean archive(final RepositoryStatsSnapshot repositoryStats) {
        assert containsRepositoryStats(repositoryStats) == false
            : "A repository with ephemeral id " + repositoryStats.getRepositoryInfo().ephemeralId + " is already archived";
        evict();

        if (archive.size() >= maxCapacity) {
            return false;
        }

        RepositoryInfo stoppedRepoInfo =
            repositoryStats.getRepositoryInfo().stopped(absoluteTimeSupplier.getAsLong());
        RepositoryStatsSnapshot stoppedStats =
            new RepositoryStatsSnapshot(stoppedRepoInfo, repositoryStats.getRepositoryStats(), relativeTimeSupplier.getAsLong());
        return archive.add(stoppedStats);
    }

    synchronized List<RepositoryStatsSnapshot> getArchivedStats() {
        evict();
        return List.copyOf(archive);
    }

    /**
     * Clears the archive, returning the valid archived entries up until that point.
     *
     * @return the repository stats that were stored before clearing the archive.
     */
    synchronized List<RepositoryStatsSnapshot> clear() {
        List<RepositoryStatsSnapshot> archivedStats = getArchivedStats();
        archive.clear();
        logger.debug("RepositoriesStatsArchive have been cleared. Removed stats: [{}]", archivedStats);
        return archivedStats;
    }

    private void evict() {
        RepositoryStatsSnapshot stats;
        while ((stats = archive.peek()) != null && stats.ageInMillis(relativeTimeSupplier) >= retentionPeriod.getMillis()) {
            RepositoryStatsSnapshot removedStats = archive.poll();
            logger.debug("Evicting repository stats [{}]", removedStats);
        }
    }

    private boolean containsRepositoryStats(RepositoryStatsSnapshot repositoryStats) {
        return archive.stream().anyMatch(r -> r.getRepositoryInfo().ephemeralId.equals(repositoryStats.getRepositoryInfo().ephemeralId));
    }
}
