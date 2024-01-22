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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.repositories.RepositoryData.CORRUPTED_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.CORRUPTED_DEFINITION;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.INVALID_DEFINITION;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.UNKNOWN_DEFINITION;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoryIntegrityHealthIndicatorServiceTests extends ESTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private HealthInfo healthInfo;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        node1 = DiscoveryNodeUtils.create(randomAlphaOfLength(10), randomUUID());
        node2 = DiscoveryNodeUtils.create(randomAlphaOfLength(10), randomUUID());
        healthInfo = new HealthInfo(
            Map.of(),
            null,
            new HashMap<>(
                Map.of(
                    node1.getId(),
                    new RepositoriesHealthInfo(List.of(), List.of()),
                    node2.getId(),
                    new RepositoriesHealthInfo(List.of(), List.of())
                )
            )
        );
    }

    public void testIsGreenWhenAllRepositoriesAreHealthy() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    RepositoryIntegrityHealthIndicatorService.ALL_REPOS_HEALTHY,
                    new SimpleHealthIndicatorDetails(Map.of("total_repositories", repos.size())),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenAtLeastOneRepoIsCorrupted() {
        var repos = appendToCopy(
            randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)),
            createRepositoryMetadata("corrupted-repo", true)
        );
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        List<String> corruptedRepos = List.of("corrupted-repo");
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] corrupted snapshot repository.",
                    createDetails(repos.size(), 1, corruptedRepos, 0, 0),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedRepos))))
                )
            )
        );
    }

    public void testIsYellowWhenAtLeastOneRepoIsUnknown() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        repos.add(createRepositoryMetadata("unknown-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of("unknown-repo"), List.of()));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] unknown snapshot repository.",
                    createDetails(repos.size(), 0, List.of(), 1, 0),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            UNKNOWN_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of("unknown-repo")),
                                new Diagnosis.Resource(List.of(node1))
                            )
                        )
                    )
                )
            )
        );
    }

    public void testIsYellowWhenAtLeastOneRepoIsInvalid() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        repos.add(createRepositoryMetadata("invalid-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of(), List.of("invalid-repo")));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] invalid snapshot repository.",
                    createDetails(repos.size(), 0, List.of(), 0, 1),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            INVALID_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of("invalid-repo")),
                                new Diagnosis.Resource(List.of(node1))
                            )
                        )
                    )
                )
            )
        );
    }

    public void testIsYellowWhenEachRepoTypeIsPresent() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        repos.add(createRepositoryMetadata("corrupted-repo", true));
        repos.add(createRepositoryMetadata("unknown-repo", false));
        repos.add(createRepositoryMetadata("invalid-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of("unknown-repo"), List.of()));
        healthInfo.repositoriesInfoByNode().put(node2.getId(), new RepositoriesHealthInfo(List.of(), List.of("invalid-repo")));

        var corrupted = List.of("corrupted-repo");
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] corrupted snapshot repository, and [1] unknown snapshot repository, and [1] invalid snapshot repository.",
                    createDetails(repos.size(), 1, corrupted, 1, 1),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corrupted))),
                        new Diagnosis(
                            UNKNOWN_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of("unknown-repo")),
                                new Diagnosis.Resource(List.of(node1))
                            )
                        ),
                        new Diagnosis(
                            INVALID_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of("invalid-repo")),
                                new Diagnosis.Resource(List.of(node2))
                            )
                        )
                    )
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(false, healthInfo),
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

    public void testLimitNumberOfAffectedResources() {
        List<RepositoryMetadata> repos = Stream.iterate(0, n -> n + 1)
            .limit(20)
            .map(i -> createRepositoryMetadata("corrupted-repo" + i, true))
            .toList();
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryCorruptionHealthIndicatorService(clusterState);

        {
            assertThat(
                service.calculate(true, 10, healthInfo).diagnosisList(),
                equalTo(
                    List.of(
                        new Diagnosis(
                            CORRUPTED_DEFINITION,
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
                service.calculate(true, 0, healthInfo).diagnosisList(),
                equalTo(List.of(new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of())))))
            );
        }
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(RepositoryIntegrityHealthIndicatorService.NAME, equalTo("repository_integrity"));
        assertThat(CORRUPTED_DEFINITION.getUniqueId(), equalTo("elasticsearch:health:repository_integrity:diagnosis:corrupt_repository"));
    }

    private ClusterState createClusterStateWith(RepositoriesMetadata metadata) {
        var builder = ClusterState.builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(Metadata.builder().putCustom(RepositoriesMetadata.TYPE, metadata));
        }
        builder.nodes(DiscoveryNodes.builder().add(node1).add(node2).build());
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

    private SimpleHealthIndicatorDetails createDetails(int total, int corruptedCount, List<String> corrupted, int unknown, int invalid) {
        return new SimpleHealthIndicatorDetails(
            Map.of(
                "total_repositories",
                total,
                "corrupted_repositories",
                corruptedCount,
                "corrupted",
                corrupted,
                "unknown_repositories",
                unknown,
                "invalid_repositories",
                invalid
            )
        );
    }
}
