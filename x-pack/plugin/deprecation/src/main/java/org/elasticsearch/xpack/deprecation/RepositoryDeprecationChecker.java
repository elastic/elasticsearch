/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryDeprecationInfo;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks registered repositories for deprecated features in use.
 */
public class RepositoryDeprecationChecker implements ResourceDeprecationChecker {

    static final String NAME = "repositories";

    private final RepositoriesService repositoriesService;

    RepositoryDeprecationChecker(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public Map<String, List<DeprecationIssue>> check(
        ProjectMetadata project,
        DeprecationInfoAction.Request request,
        TransportDeprecationInfoAction.PrecomputedData precomputedData
    ) {
        final Map<String, List<DeprecationIssue>> issues = new HashMap<>();
        for (Repository repository : repositoriesService.getProjectRepositories(project.id()).values()) {
            final List<DeprecationIssue> repositoryIssues = repository.getDeprecationInfos()
                .stream()
                .map(RepositoryDeprecationChecker::toDeprecationIssue)
                .toList();
            if (repositoryIssues.isEmpty() == false) {
                issues.put(repository.getMetadata().name(), repositoryIssues);
            }
        }
        return issues;
    }

    private static DeprecationIssue toDeprecationIssue(RepositoryDeprecationInfo info) {
        return new DeprecationIssue(
            toDeprecationIssueLevel(info.level()),
            info.message(),
            info.referenceDocs().toString(),
            info.details(),
            info.resolveDuringRollingUpgrade(),
            null
        );
    }

    private static DeprecationIssue.Level toDeprecationIssueLevel(RepositoryDeprecationInfo.Level level) {
        return switch (level) {
            case WARNING -> DeprecationIssue.Level.WARNING;
            case CRITICAL -> DeprecationIssue.Level.CRITICAL;
        };
    }

    @Override
    public String getName() {
        return NAME;
    }
}
