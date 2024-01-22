/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.check;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.UnknownTypeRepository;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RepositoriesCheckTests extends ESTestCase {

    private RepositoriesCheck repositoriesCheck;

    private RepositoriesService repositoriesService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        repositoriesService = mock(RepositoriesService.class);

        repositoriesCheck = new RepositoriesCheck(repositoriesService);
    }

    public void test_getHealth_noRepos() {
        when(repositoriesService.getRepositories()).thenReturn(Map.of());

        var health = repositoriesCheck.getHealth();

        assertTrue(health.unknownRepositories().isEmpty());
        assertTrue(health.invalidRepositories().isEmpty());
    }

    public void test_getHealth_correctRepo() {
        var metadata = mock(RepositoryMetadata.class);
        // generation should be != RepositoryData.UNKNOWN_REPO_GEN which is equal to -2.
        when(metadata.generation()).thenReturn(randomNonNegativeLong());
        var repo = mock(Repository.class);
        when(repo.getMetadata()).thenReturn(metadata);
        when(repositoriesService.getRepositories())
            .thenReturn(Map.of(randomAlphaOfLength(10), repo));

        var health = repositoriesCheck.getHealth();

        assertTrue(health.unknownRepositories().isEmpty());
        assertTrue(health.invalidRepositories().isEmpty());
    }

    public void test_getHealth_unknownType() {
        when(repositoriesService.getRepositories())
            .thenReturn(Map.of(randomAlphaOfLength(10), new UnknownTypeRepository(createRepositoryMetadata())));

        var health = repositoriesCheck.getHealth();

        assertFalse(health.unknownRepositories().isEmpty());
        assertTrue(health.invalidRepositories().isEmpty());
    }

    public void test_getHealth_invalid() {
        var repoName = randomAlphaOfLength(10);
        when(repositoriesService.getRepositories())
            .thenReturn(Map.of(repoName, new InvalidRepository(createRepositoryMetadata(), new RepositoryException(repoName, "Test"))));

        var health = repositoriesCheck.getHealth();

        assertTrue(health.unknownRepositories().isEmpty());
        assertEquals(1, health.invalidRepositories().size());
    }

    public void test_setBuilder() {
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new RepositoriesHealthInfo(List.of(), List.of());

        repositoriesCheck.setBuilder(builder, health);

        verify(builder).setRepositoriesHealthInfo(health);
    }

    private static RepositoryMetadata createRepositoryMetadata() {
        var generation = randomNonNegativeLong() / 2L;
        return new RepositoryMetadata(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Settings.EMPTY,
            generation,
            generation + randomLongBetween(0, generation)
        );
    }
}
