/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.ClusterState;
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

    public void testAll() {
        runMatchAllTest();
        runMatchAllTest("*");
        runMatchAllTest("_all");
    }

    private static void runMatchAllTest(String... patterns) {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var result = getRepositories(state, patterns);
        assertEquals(RepositoriesMetadata.get(state).repositories(), result.repositoryMetadata());
        assertThat(result.missing(), Matchers.empty());
        assertFalse(result.hasMissingRepositories());
    }

    public void testMatchingName() {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var name = randomFrom(RepositoriesMetadata.get(state).repositories()).name();
        final var result = getRepositories(state, name);
        assertEquals(List.of(RepositoriesMetadata.get(state).repository(name)), result.repositoryMetadata());
        assertThat(result.missing(), Matchers.empty());
        assertFalse(result.hasMissingRepositories());
    }

    public void testMismatchingName() {
        final var state = clusterStateWithRepositories(randomList(1, 4, ESTestCase::randomIdentifier).toArray(String[]::new));
        final var notAName = randomValueOtherThanMany(
            n -> RepositoriesMetadata.get(state).repositories().stream().anyMatch(m -> n.equals(m.name())),
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

    private static void runWildcardTest(ClusterState clusterState, List<String> expectedNames, String... patterns) {
        final var result = getRepositories(clusterState, patterns);
        final var description = Strings.format("%s should yield %s", Arrays.toString(patterns), expectedNames);
        assertFalse(description, result.hasMissingRepositories());
        assertEquals(description, expectedNames, result.repositoryMetadata().stream().map(RepositoryMetadata::name).toList());
    }

    private static ResolvedRepositories getRepositories(ClusterState clusterState, String... patterns) {
        return ResolvedRepositories.resolve(clusterState, patterns);
    }

    private static ClusterState clusterStateWithRepositories(String... repoNames) {
        final var repositories = new ArrayList<RepositoryMetadata>(repoNames.length);
        for (final var repoName : repoNames) {
            repositories.add(new RepositoryMetadata(repoName, "test", Settings.EMPTY));
        }
        return ClusterState.EMPTY_STATE.copyAndUpdateMetadata(
            b -> b.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositories))
        );
    }

}
