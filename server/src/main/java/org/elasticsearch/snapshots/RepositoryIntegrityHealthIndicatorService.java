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
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.repositories.RepositoryData;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

public class RepositoryIntegrityHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "repository integrity";

    private final ClusterService clusterService;

    public RepositoryIntegrityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var snapshotMetadata = clusterService.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

        if (snapshotMetadata.repositories().isEmpty()) {
            return new HealthIndicatorResult(NAME, DATA, GREEN, "No repositories configured", null);
        }

        var corrupted = snapshotMetadata.repositories()
            .stream()
            .filter(repository -> repository.generation() == RepositoryData.CORRUPTED_REPO_GEN)
            .map(RepositoryMetadata::name)
            .toList();

        if (corrupted.isEmpty()) {
            return new HealthIndicatorResult(NAME, DATA, GREEN, "", null);
        }

        // TODO 83303 add details with count and truncated list
        return new HealthIndicatorResult(
            NAME,
            DATA,
            RED,
            "Detected corrupted " + (corrupted.size() == 1 ? "repository" : "repositories"),
            null
        );
    }
}
