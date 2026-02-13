/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResolvedRepositoriesTests extends ESTestCase {

    private ProjectId projectId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
    }

    public void testAll() {
        runMatchAllTest();
        runMatchAllTest("*");
        runMatchAllTest("_all");
    }

    private void runMatchAllTest(String... patterns) {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var result = getRepositories(state, patterns);
        assertEquals(RepositoriesMetadata.get(state.metadata().getProject(projectId)).repositories(), result.repositoryMetadata());
        assertThat(result.missing(), Matchers.empty());
        assertFalse(result.hasMissingRepositories());
    }

    public void testMatchingName() {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var name = randomFrom(RepositoriesMetadata.get(state.metadata().getProject(projectId)).repositories()).name();
        final var result = getRepositories(state, name);
        assertEquals(
            List.of(RepositoriesMetadata.get(state.metadata().getProject(projectId)).repository(name)),
            result.repositoryMetadata()
        );
        assertThat(result.missing(), Matchers.empty());
        assertFalse(result.hasMissingRepositories());
    }

    public void testMismatchingName() {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var notAName = randomValueOtherThanMany(
            n -> RepositoriesMetadata.get(state.metadata().getProject(projectId)).repositories().stream().anyMatch(m -> n.equals(m.name())),
            ESTestCase::randomIdentifier
        );
        final var result = getRepositories(state, notAName);
        assertEquals(List.of(), result.repositoryMetadata());
        assertEquals(List.of(notAName), result.missing());
        assertTrue(result.hasMissingRepositories());
    }

    public void testWildcards() {
        final var state = clusterStateWithRepositories("test-match-1", "test-match-2", "test-exclude", "other-repo");

        runWildcardTest(state, List.of("test-match-1", "test-match-2", "test-exclude"), "test-*");
        runWildcardTest(state, List.of("test-match-1", "test-match-2"), "test-*1", "test-*2");
        runWildcardTest(state, List.of("test-match-2", "test-match-1"), "test-*2", "test-*1");
        runWildcardTest(state, List.of("test-match-1", "test-match-2"), "test-*", "-*-exclude");
        runWildcardTest(state, List.of(), "no-*-repositories");
        runWildcardTest(state, List.of("test-match-1", "test-match-2", "other-repo"), "test-*", "-*-exclude", "other-repo");
        runWildcardTest(state, List.of("other-repo", "test-match-1", "test-match-2"), "other-repo", "test-*", "-*-exclude");
    }

    private void runWildcardTest(ClusterState clusterState, List<String> expectedNames, String... patterns) {
        final var result = getRepositories(clusterState, patterns);
        final var description = Strings.format("%s should yield %s", Arrays.toString(patterns), expectedNames);
        assertFalse(description, result.hasMissingRepositories());
        assertEquals(description, expectedNames, result.repositoryMetadata().stream().map(RepositoryMetadata::name).toList());
    }

    private ResolvedRepositories getRepositories(ClusterState clusterState, String... patterns) {
        return ResolvedRepositories.resolve(clusterState.metadata().getProject(projectId), patterns);
    }

    private ClusterState clusterStateWithRepositories(String... repoNames) {
        final var repositories = new ArrayList<RepositoryMetadata>(repoNames.length);
        for (final var repoName : repoNames) {
            repositories.add(new RepositoryMetadata(repoName, "test", Settings.EMPTY));
        }
        return ClusterState.EMPTY_STATE.copyAndUpdateMetadata(b -> {
            ProjectMetadata.Builder projectBuilder = b.getProject(projectId);
            if (projectBuilder == null) {
                projectBuilder = ProjectMetadata.builder(projectId);
            }
            b.put(projectBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositories)));
        });
    }

}
