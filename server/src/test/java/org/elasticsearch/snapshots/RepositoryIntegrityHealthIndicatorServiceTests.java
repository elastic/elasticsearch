/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.elasticsearch.repositories.RepositoryData.CORRUPTED_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoryIntegrityHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenAllRepositoriesAreNotCorrupted() {
        var clusterState = createClusterStateWith(
            new RepositoriesMetadata(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)))
        );
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(service.calculate(), equalTo(new HealthIndicatorResult(NAME, DATA, GREEN, "", null)));
    }

    public void testIsRedWhenAtLeastOneRepoIsCorrupted() {
        var clusterState = createClusterStateWith(
            new RepositoriesMetadata(
                appendToCopy(
                    randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)),
                    createRepositoryMetadata("corrupted-repo", true)
                )
            )
        );
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(service.calculate(), equalTo(new HealthIndicatorResult(NAME, DATA, RED, "Detected corrupted repository", null)));
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(service.calculate(), equalTo(new HealthIndicatorResult(NAME, DATA, GREEN, "No repositories configured", null)));
    }

    private static ClusterState createClusterStateWith(RepositoriesMetadata metadata) {
        var builder = ClusterState.builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(Metadata.builder().putCustom(RepositoriesMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static RepositoryMetadata createRepositoryMetadata(String name, boolean corrupted) {
        return new RepositoryMetadata(name, "uuid", "s3", Settings.EMPTY, corrupted ? CORRUPTED_REPO_GEN : EMPTY_REPO_GEN, EMPTY_REPO_GEN);
    }

    private static RepositoryIntegrityHealthIndicatorService createRepositoryCorruptionHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new RepositoryIntegrityHealthIndicatorService(clusterService);
    }
}
