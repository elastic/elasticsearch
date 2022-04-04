/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.repositories.RepositoryData;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.collectionToDelimitedStringWithLimit;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;

/**
 * This indicator reports health for snapshot repositories.
 *
 * Indicator will report RED status when any of snapshot repositories is marked as corrupted.
 * Data might not be backed up in such cases.
 *
 * Corrupted repository most likely need to be manually cleaned and a new snapshot needs to be created from scratch.
 */
public class RepositoryIntegrityHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "repository_integrity";

    private final ClusterService clusterService;

    public RepositoryIntegrityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return SNAPSHOT;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var snapshotMetadata = clusterService.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

        if (snapshotMetadata.repositories().isEmpty()) {
            return createIndicator(GREEN, "No repositories configured.", HealthIndicatorDetails.EMPTY, Collections.emptyList());
        }

        var corrupted = snapshotMetadata.repositories()
            .stream()
            .filter(repository -> repository.generation() == RepositoryData.CORRUPTED_REPO_GEN)
            .map(RepositoryMetadata::name)
            .toList();

        var totalRepositories = snapshotMetadata.repositories().size();
        var corruptedRepositories = corrupted.size();

        if (corrupted.isEmpty()) {
            return createIndicator(
                GREEN,
                "No corrupted repositories.",
                new SimpleHealthIndicatorDetails(Map.of("total_repositories", totalRepositories)),
                Collections.emptyList()
            );
        }

        return createIndicator(
            RED,
            createCorruptedRepositorySummary(corrupted),
            new SimpleHealthIndicatorDetails(
                Map.of(
                    "total_repositories",
                    totalRepositories,
                    "corrupted_repositories",
                    corruptedRepositories,
                    "corrupted",
                    limitSize(corrupted, 10)
                )
            ),
            Collections.emptyList()
        );
    }

    private static String createCorruptedRepositorySummary(List<String> corrupted) {
        var message = new StringBuilder().append("Detected [").append(corrupted.size()).append("] corrupted repositories: ");
        collectionToDelimitedStringWithLimit(corrupted, ",", "[", "].", 1024, message);
        return message.toString();
    }
}
