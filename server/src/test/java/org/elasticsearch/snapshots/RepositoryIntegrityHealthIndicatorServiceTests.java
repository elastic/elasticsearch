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
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.repositories.RepositoryData.CORRUPTED_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.CORRUPTED_REPOSITORY;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoryIntegrityHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenAllRepositoriesAreNotCorrupted() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    RepositoryIntegrityHealthIndicatorService.NO_CORRUPT_REPOS,
                    new SimpleHealthIndicatorDetails(Map.of("total_repositories", repos.size())),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsRedWhenAtLeastOneRepoIsCorrupted() {
        var repos = appendToCopy(
            randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)),
            createRepositoryMetadata("corrupted-repo", true)
        );
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        List<String> corruptedRepos = List.of("corrupted-repo");
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    RED,
                    "Detected [1] corrupted snapshot repositories: [corrupted-repo].",
                    new SimpleHealthIndicatorDetails(
                        Map.of("total_repositories", repos.size(), "corrupted_repositories", 1, "corrupted", corruptedRepos)
                    ),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            NAME,
                            RepositoryIntegrityHealthIndicatorService.REPOSITORY_CORRUPTED_IMPACT_ID,
                            1,
                            "Data in corrupted snapshot repository [corrupted-repo] may be lost and cannot be restored.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(new Diagnosis(CORRUPTED_REPOSITORY, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedRepos))))
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(false, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    RepositoryIntegrityHealthIndicatorService.NO_REPOS_CONFIGURED,
                    HealthIndicatorDetails.EMPTY,
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(RepositoryIntegrityHealthIndicatorService.NAME, equalTo("repository_integrity"));
        assertThat(
            CORRUPTED_REPOSITORY.getUniqueId(),
            equalTo("elasticsearch:health:repository_integrity:diagnosis:corrupt_repo_integrity")
        );
    }

    public void testLimitNumberOfAffectedResources() {
        List<RepositoryMetadata> repos = Stream.iterate(0, n -> n + 1)
            .limit(20)
            .map(i -> createRepositoryMetadata("corrupted-repo" + i, true))
            .toList();
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        {
            assertThat(
                service.calculate(true, 10, HealthInfo.EMPTY_HEALTH_INFO).diagnosisList(),
                equalTo(
                    List.of(
                        new Diagnosis(
                            CORRUPTED_REPOSITORY,
                            List.of(
                                new Diagnosis.Resource(
                                    Type.SNAPSHOT_REPOSITORY,
                                    repos.stream().limit(10).map(RepositoryMetadata::name).toList()
                                )
                            )
                        )
                    )
                )
            );
        }

        {
            assertThat(
                service.calculate(true, 0, HealthInfo.EMPTY_HEALTH_INFO).diagnosisList(),
                equalTo(List.of(new Diagnosis(CORRUPTED_REPOSITORY, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of())))))
            );
        }

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
