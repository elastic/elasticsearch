/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryDeprecationInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoryDeprecationCheckerTests extends ESTestCase {

    public void testCheckRepositoryDeprecations() {
        final ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        final Repository repositoryWithDeprecation = mockRepository(
            "deprecated-repository",
            List.of(
                new RepositoryDeprecationInfo(
                    RepositoryDeprecationInfo.Level.CRITICAL,
                    "critical repository deprecation",
                    ReferenceDocs.SECURE_SETTINGS,
                    "critical details",
                    false
                ),
                new RepositoryDeprecationInfo(
                    RepositoryDeprecationInfo.Level.WARNING,
                    "warning repository deprecation",
                    ReferenceDocs.TROUBLESHOOT_REPOSITORY,
                    "warning details",
                    true
                )
            )
        );
        final Repository repositoryWithoutDeprecation = mockRepository("healthy-repository", List.of());
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        final String repositoryWithDeprecationName = repositoryWithDeprecation.getMetadata().name();
        final String repositoryWithoutDeprecationName = repositoryWithoutDeprecation.getMetadata().name();
        when(repositoriesService.getProjectRepositories(project.id())).thenReturn(
            Map.of(repositoryWithDeprecationName, repositoryWithDeprecation, repositoryWithoutDeprecationName, repositoryWithoutDeprecation)
        );

        final Map<String, List<DeprecationIssue>> issues = new RepositoryDeprecationChecker(repositoriesService).check(
            project,
            new DeprecationInfoAction.Request(randomTimeValue()),
            new TransportDeprecationInfoAction.PrecomputedData(null)
        );

        assertThat(issues.keySet(), equalTo(Set.of("deprecated-repository")));
        assertThat(
            issues.get("deprecated-repository"),
            equalTo(
                List.of(
                    new DeprecationIssue(
                        DeprecationIssue.Level.CRITICAL,
                        "critical repository deprecation",
                        ReferenceDocs.SECURE_SETTINGS.toString(),
                        "critical details",
                        false,
                        null
                    ),
                    new DeprecationIssue(
                        DeprecationIssue.Level.WARNING,
                        "warning repository deprecation",
                        ReferenceDocs.TROUBLESHOOT_REPOSITORY.toString(),
                        "warning details",
                        true,
                        null
                    )
                )
            )
        );
    }

    public void testCheckRepositoryDeprecationsReturnsEmptyMap() {
        final ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        final Repository healthyRepository = mockRepository("healthy-repository", List.of());
        when(repositoriesService.getProjectRepositories(project.id())).thenReturn(Map.of("healthy-repository", healthyRepository));

        assertThat(
            new RepositoryDeprecationChecker(repositoriesService).check(
                project,
                new DeprecationInfoAction.Request(randomTimeValue()),
                new TransportDeprecationInfoAction.PrecomputedData(null)
            ).isEmpty(),
            is(true)
        );
    }

    private static Repository mockRepository(String name, List<RepositoryDeprecationInfo> deprecationInfos) {
        final Repository repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(name, "test", Settings.EMPTY));
        when(repository.getDeprecationInfos()).thenReturn(deprecationInfos);
        return repository;
    }
}
