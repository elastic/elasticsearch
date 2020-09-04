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
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class RepositoriesStatsArchive {
    private static final Logger logger = LogManager.getLogger(RepositoriesStatsArchive.class);

    private final TimeValue retentionPeriod;
    private final int maxCapacity;
    private final LongSupplier relativeTimeSupplier;
    private final Deque<ArchiveEntry> archive = new ArrayDeque<>();

    public RepositoriesStatsArchive(TimeValue retentionPeriod,
                                    int maxCapacity,
                                    LongSupplier relativeTimeSupplier) {
        this.retentionPeriod = retentionPeriod;
        this.maxCapacity = maxCapacity;
        this.relativeTimeSupplier = relativeTimeSupplier;
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
        assert repositoryStats.isArchived();

        evict();

        if (archive.size() >= maxCapacity) {
            return false;
        }

        return archive.add(new ArchiveEntry(repositoryStats, relativeTimeSupplier.getAsLong()));
    }

    synchronized List<RepositoryStatsSnapshot> getArchivedStats() {
        evict();
        return archive.stream().map(e -> e.repositoryStatsSnapshot).collect(Collectors.toList());
    }

    /**
     * Clears the archive, returning the valid archived entries up until that point.
     *
     * @return the repository stats that were stored before clearing the archive.
     */
    synchronized List<RepositoryStatsSnapshot> clear(long maxVersionToClear) {
        List<RepositoryStatsSnapshot> clearedStats = new ArrayList<>();
        Iterator<ArchiveEntry> iterator = archive.iterator();
        while (iterator.hasNext()) {
            RepositoryStatsSnapshot statsSnapshot = iterator.next().repositoryStatsSnapshot;
            if (statsSnapshot.getClusterVersion() <= maxVersionToClear) {
                clearedStats.add(statsSnapshot);
                iterator.remove();
            }
        }
        logger.debug("RepositoriesStatsArchive have been cleared. Removed stats: [{}]", clearedStats);
        return clearedStats;
    }

    private void evict() {
        ArchiveEntry entry;
        while ((entry = archive.peek()) != null && entry.ageInMillis(relativeTimeSupplier) >= retentionPeriod.getMillis()) {
            ArchiveEntry removedEntry = archive.poll();
            logger.debug("Evicting repository stats [{}]", removedEntry.repositoryStatsSnapshot);
        }
    }

    private boolean containsRepositoryStats(RepositoryStatsSnapshot repositoryStats) {
        return archive.stream()
            .anyMatch(entry ->
                entry.repositoryStatsSnapshot.getRepositoryInfo().ephemeralId.equals(repositoryStats.getRepositoryInfo().ephemeralId));
    }

    private static class ArchiveEntry {
        private final RepositoryStatsSnapshot repositoryStatsSnapshot;
        private final long createdAtMillis;

        private ArchiveEntry(RepositoryStatsSnapshot repositoryStatsSnapshot, long createdAtMillis) {
            this.repositoryStatsSnapshot = repositoryStatsSnapshot;
            this.createdAtMillis = createdAtMillis;
        }

        private long ageInMillis(LongSupplier relativeTimeInMillis) {
            return Math.max(0, relativeTimeInMillis.getAsLong() - createdAtMillis);
        }
    }
}
