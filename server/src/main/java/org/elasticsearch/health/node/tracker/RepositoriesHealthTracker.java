/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.health.node.HealthIndicatorDisplayValues;
import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.UnknownTypeRepository;

import java.util.ArrayList;
import java.util.List;

/**
 * Determines the health of repositories on this node.
 */
public class RepositoriesHealthTracker extends HealthTracker<RepositoriesHealthInfo> {
    private final RepositoriesService repositoriesService;
    private final ProjectResolver projectResolver;

    public RepositoriesHealthTracker(RepositoriesService repositoriesService, ProjectResolver projectResolver) {
        this.repositoriesService = repositoriesService;
        this.projectResolver = projectResolver;
    }

    /**
     * Determine the health of the repositories on this node. Do so by checking the current collection of registered repositories.
     *
     * @return the current repositories health on this node.
     */
    @Override
    protected RepositoriesHealthInfo determineCurrentHealth() {
        var repositories = repositoriesService.getRepositories();
        if (repositories.isEmpty()) {
            return new RepositoriesHealthInfo(List.of(), List.of());
        }

        boolean supportsMultipleProjects = projectResolver.supportsMultipleProjects();
        var unknown = new ArrayList<String>();
        var invalid = new ArrayList<String>();
        repositories.forEach(repository -> {
            String displayName = HealthIndicatorDisplayValues.getRepositoryDisplayName(
                repository.getProjectId(),
                repository.getMetadata().name(),
                supportsMultipleProjects
            );
            if (repository instanceof UnknownTypeRepository) {
                unknown.add(displayName);
            } else if (repository instanceof InvalidRepository) {
                invalid.add(displayName);
            }
        });
        return new RepositoriesHealthInfo(unknown, invalid);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, RepositoriesHealthInfo healthInfo) {
        builder.repositoriesHealthInfo(healthInfo);
    }
}
