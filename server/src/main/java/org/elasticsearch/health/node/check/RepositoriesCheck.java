/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.check;

import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.UnknownTypeRepository;

import java.util.ArrayList;

/**
 * Determines the health of repositories on this node.
 */
public class RepositoriesCheck implements HealthCheck<RepositoriesHealthInfo> {
    private final RepositoriesService repositoriesService;

    public RepositoriesCheck(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public RepositoriesHealthInfo getHealth() {
        var unknown = new ArrayList<String>();
        var invalid = new ArrayList<String>();
        repositoriesService.getRepositories().forEach((name, repository) -> {
            if (repository instanceof UnknownTypeRepository) {
                unknown.add(repository.getMetadata().name());
            } else if (repository instanceof InvalidRepository) {
                invalid.add(repository.getMetadata().name());
            }
        });
        return new RepositoriesHealthInfo(unknown, invalid);
    }

    @Override
    public void addHealthToBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, RepositoriesHealthInfo healthInfo) {
        builder.repositoriesHealthInfo(healthInfo);
    }
}
