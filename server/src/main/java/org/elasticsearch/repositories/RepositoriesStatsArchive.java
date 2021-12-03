/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;

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

    public RepositoriesStatsArchive(TimeValue retentionPeriod, int maxCapacity, LongSupplier relativeTimeSupplier) {
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
            .anyMatch(
                entry -> entry.repositoryStatsSnapshot.getRepositoryInfo().ephemeralId.equals(
                    repositoryStats.getRepositoryInfo().ephemeralId
                )
            );
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
