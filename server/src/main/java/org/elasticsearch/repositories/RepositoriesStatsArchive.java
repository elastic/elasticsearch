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

import org.elasticsearch.common.unit.TimeValue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public final class RepositoriesStatsArchive {
    private final TimeValue retentionPeriod;
    private final Deque<RepositoryStatsSnapshot> archive = new ArrayDeque<>();

    RepositoriesStatsArchive(TimeValue retentionPeriod) {
        this.retentionPeriod = retentionPeriod;
    }

    synchronized void archive(RepositoryStatsSnapshot repositoryStats) {
        if (repositoryStats.getRepositoryInfo().isStopped() == false) {
            repositoryStats = repositoryStats.withStoppedRepo();
        }
        archive.add(repositoryStats);
        evict();
    }

    synchronized List<RepositoryStatsSnapshot> getArchivedStats() {
        evict();
        return List.copyOf(archive);
    }

    synchronized void clear() {
        archive.clear();
    }

    private void evict() {
        Instant retentionDeadline = getRetentionDeadline();
        RepositoryStatsSnapshot stats;
        while ((stats = archive.peek()) != null && shouldEvict(stats, retentionDeadline)) {
            archive.poll();
        }
    }

    private boolean shouldEvict(RepositoryStatsSnapshot stats, Instant deadline) {
        return stats.wasRepoStoppedBefore(deadline);
    }

    private Instant getRetentionDeadline() {
        return Instant.now().minus(Duration.ofMillis(retentionPeriod.getMillis()));
    }
}
