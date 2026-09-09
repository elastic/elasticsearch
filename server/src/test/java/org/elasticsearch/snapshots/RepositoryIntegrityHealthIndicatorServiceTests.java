/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.UNKNOWN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.repositories.RepositoryData.CORRUPTED_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.CORRUPTED_DEFINITION;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.INVALID_DEFINITION;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.UNKNOWN_DEFINITION;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoryIntegrityHealthIndicatorServiceTests extends ESTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private HealthInfo healthInfo;
    private FeatureService featureService;

    @Before
    public void initNodes() {
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
            ),
            FileSettingsHealthInfo.INDETERMINATE
        );

        featureService = Mockito.mock(FeatureService.class);
        Mockito.when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
    }

    public void testIsGreenWhenAllRepositoriesAreHealthy() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);

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
        var corruptedRepos = createNamedRepositories("corrupted-repo-", true);
        var repos = concatLists(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)), corruptedRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);

        List<String> corruptedNames = names(corruptedRepos);
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), 0, 0),
                    createDetails(repos.size(), corruptedNames.size(), corruptedNames, 0, 0),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedNames))))
                )
            )
        );
    }

    public void testIsYellowWhenAtLeastOneRepoIsUnknown() {
        var unknownRepos = createNamedRepositories("unknown-repo-", false);
        var repos = concatLists(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)), unknownRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        List<String> unknownNames = names(unknownRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(unknownNames, List.of()));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(0, unknownNames.size(), 0),
                    createDetails(repos.size(), 0, List.of(), unknownNames.size(), 0),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            UNKNOWN_DEFINITION,
                            List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, unknownNames), new Diagnosis.Resource(List.of(node1)))
                        )
                    )
                )
            )
        );
    }

    public void testIsYellowWhenAtLeastOneRepoIsInvalid() {
        var invalidRepos = createNamedRepositories("invalid-repo-", false);
        var repos = concatLists(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)), invalidRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        List<String> invalidNames = names(invalidRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of(), invalidNames));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(0, 0, invalidNames.size()),
                    createDetails(repos.size(), 0, List.of(), 0, invalidNames.size()),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            INVALID_DEFINITION,
                            List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, invalidNames), new Diagnosis.Resource(List.of(node1)))
                        )
                    )
                )
            )
        );
    }

    public void testIsYellowWhenEachRepoTypeIsPresent() {
        var corruptedRepos = createNamedRepositories("corrupted-repo-", true);
        var unknownRepos = createNamedRepositories("unknown-repo-", false);
        var invalidRepos = createNamedRepositories("invalid-repo-", false);
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        repos.addAll(corruptedRepos);
        repos.addAll(unknownRepos);
        repos.addAll(invalidRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        List<String> corruptedNames = names(corruptedRepos);
        List<String> unknownNames = names(unknownRepos);
        List<String> invalidNames = names(invalidRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(unknownNames, List.of()));
        healthInfo.repositoriesInfoByNode().put(node2.getId(), new RepositoriesHealthInfo(List.of(), invalidNames));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), unknownNames.size(), invalidNames.size()),
                    createDetails(repos.size(), corruptedNames.size(), corruptedNames, unknownNames.size(), invalidNames.size()),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedNames))),
                        new Diagnosis(
                            UNKNOWN_DEFINITION,
                            List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, unknownNames), new Diagnosis.Resource(List.of(node1)))
                        ),
                        new Diagnosis(
                            INVALID_DEFINITION,
                            List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, invalidNames), new Diagnosis.Resource(List.of(node2)))
                        )
                    )
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);

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

    public void testIsUnknownWhenNoHealthInfoIsAvailable() {
        var repos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        var verbose = randomBoolean();

        assertThat(
            service.calculate(verbose, new HealthInfo(Map.of(), null, Map.of(), FileSettingsHealthInfo.INDETERMINATE)),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    UNKNOWN,
                    RepositoryIntegrityHealthIndicatorService.NO_REPO_HEALTH_INFO,
                    verbose
                        ? new SimpleHealthIndicatorDetails(
                            Map.of("total_repositories", repos.size(), "corrupted_repositories", 0, "corrupted", List.of())
                        )
                        : HealthIndicatorDetails.EMPTY,
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testLimitNumberOfAffectedResources() {
        var ids = Stream.iterate(0, n -> n + 1).limit(20).toList();
        List<RepositoryMetadata> repos = ids.stream().map(i -> createRepositoryMetadata("corrupted-repo" + i, true)).toList();
        // Create nodes
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        ids.forEach(i -> discoveryNodesBuilder.add(DiscoveryNodeUtils.create(randomAlphaOfLength(10), "node-" + i)));
        var nodes = discoveryNodesBuilder.build();
        // Create state & service
        var clusterState = ClusterState.builder(createClusterStateWith(new RepositoriesMetadata(repos))).nodes(nodes).build();
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        // Create repos
        final List<String> unknownRepos = new ArrayList<>();
        final List<String> invalidRepos = new ArrayList<>();
        Map<String, RepositoriesHealthInfo> repoHealthInfo = new HashMap<>();
        ids.forEach(i -> {
            unknownRepos.add("unknown-repo-" + i);
            invalidRepos.add("invalid-repo-" + i);
            repoHealthInfo.put("node-" + i, new RepositoriesHealthInfo(List.of("unknown-repo-" + i), List.of("invalid-repo-" + i)));
        });
        healthInfo = new HealthInfo(
            healthInfo.diskInfoByNode(),
            healthInfo.dslHealthInfo(),
            repoHealthInfo,
            FileSettingsHealthInfo.INDETERMINATE
        );

        List<String> expectedCorruptedDetails = repos.stream().map(RepositoryMetadata::name).sorted().limit(10).toList();
        var expectedDetails = createDetails(repos.size(), repos.size(), expectedCorruptedDetails, unknownRepos.size(), invalidRepos.size());

        var resultLimitedToTen = service.calculate(true, 10, healthInfo);
        assertThat(resultLimitedToTen.diagnosisList(), equalTo(createDiagnoses(repos, nodes, unknownRepos, invalidRepos, 10)));
        assertThat(resultLimitedToTen.details(), equalTo(expectedDetails));

        var resultLimitedToZero = service.calculate(true, 0, healthInfo);
        assertThat(resultLimitedToZero.diagnosisList(), equalTo(createDiagnoses(repos, nodes, unknownRepos, invalidRepos, 0)));
        assertThat(resultLimitedToZero.details(), equalTo(expectedDetails));
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        var repos = appendToCopy(
            randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)),
            createRepositoryMetadata("corrupted-repo", true)
        );
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(false, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] corrupted snapshot repository.",
                    HealthIndicatorDetails.EMPTY,
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of()
                )
            )
        );
    }

    public void testUnhealthyRepositoryReportedByMultipleNodesIsDeduplicated() {
        boolean unknown = randomBoolean();
        String repoName = unknown ? "unknown-repo" : "invalid-repo";
        var clusterState = createClusterStateWith(new RepositoriesMetadata(List.of(createRepositoryMetadata(repoName, false))));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        var repoHealth = unknown
            ? new RepositoriesHealthInfo(List.of(repoName), List.of())
            : new RepositoriesHealthInfo(List.of(), List.of(repoName));
        healthInfo.repositoriesInfoByNode().put(node1.getId(), repoHealth);
        healthInfo.repositoriesInfoByNode().put(node2.getId(), repoHealth);

        List<DiscoveryNode> expectedNodes = Stream.of(node1, node2).sorted(DISCOVERY_NODE_COMPARATOR).toList();
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Detected [1] " + (unknown ? "unknown" : "invalid") + " snapshot repository.",
                    createDetails(1, 0, List.of(), unknown ? 1 : 0, unknown ? 0 : 1),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            unknown ? UNKNOWN_DEFINITION : INVALID_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of(repoName)),
                                new Diagnosis.Resource(expectedNodes)
                            )
                        )
                    )
                )
            )
        );
    }

    private List<Diagnosis> createDiagnoses(
        List<RepositoryMetadata> repos,
        DiscoveryNodes nodes,
        List<String> unknownRepos,
        List<String> invalidRepos,
        int maxAffectedResourcesCount
    ) {
        return List.of(
            new Diagnosis(
                CORRUPTED_DEFINITION,
                List.of(
                    new Diagnosis.Resource(
                        Type.SNAPSHOT_REPOSITORY,
                        repos.stream().map(RepositoryMetadata::name).sorted().limit(maxAffectedResourcesCount).toList()
                    )
                )
            ),
            new Diagnosis(
                UNKNOWN_DEFINITION,
                List.of(
                    new Diagnosis.Resource(
                        Type.SNAPSHOT_REPOSITORY,
                        unknownRepos.stream().sorted().limit(maxAffectedResourcesCount).toList()
                    ),
                    new Diagnosis.Resource(
                        nodes.getAllNodes().stream().sorted(DISCOVERY_NODE_COMPARATOR).limit(maxAffectedResourcesCount).toList()
                    )
                )
            ),
            new Diagnosis(
                INVALID_DEFINITION,
                List.of(
                    new Diagnosis.Resource(
                        Type.SNAPSHOT_REPOSITORY,
                        invalidRepos.stream().sorted().limit(maxAffectedResourcesCount).toList()
                    ),
                    new Diagnosis.Resource(
                        nodes.getAllNodes().stream().sorted(DISCOVERY_NODE_COMPARATOR).limit(maxAffectedResourcesCount).toList()
                    )
                )
            )
        );
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertEquals("repository_integrity", RepositoryIntegrityHealthIndicatorService.NAME);
        assertEquals("elasticsearch:health:repository_integrity:diagnosis:corrupt_repo_integrity", CORRUPTED_DEFINITION.getUniqueId());
        assertEquals("elasticsearch:health:repository_integrity:diagnosis:unknown_repository", UNKNOWN_DEFINITION.getUniqueId());
        assertEquals("elasticsearch:health:repository_integrity:diagnosis:invalid_repository", INVALID_DEFINITION.getUniqueId());
    }

    private ClusterState createClusterStateWith(RepositoriesMetadata metadata) {
        var builder = ClusterState.builder(new ClusterName("test-cluster")).nodes(DiscoveryNodes.builder().add(node1).add(node2).build());
        if (metadata != null) {
            builder.metadata(Metadata.builder().putCustom(RepositoriesMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static RepositoryMetadata createRepositoryMetadata(String name, boolean corrupted) {
        return new RepositoryMetadata(name, "uuid", "s3", Settings.EMPTY, corrupted ? CORRUPTED_REPO_GEN : EMPTY_REPO_GEN, EMPTY_REPO_GEN);
    }

    private static List<RepositoryMetadata> createNamedRepositories(String namePrefix, boolean corrupted) {
        return IntStream.range(0, randomIntBetween(1, 10)).mapToObj(i -> createRepositoryMetadata(namePrefix + i, corrupted)).toList();
    }

    private static List<String> names(List<RepositoryMetadata> repos) {
        return repos.stream().map(RepositoryMetadata::name).sorted().toList();
    }

    private static String expectedSymptom(int corrupted, int unknown, int invalid) {
        return "Detected "
            + Stream.of(symptomPart("corrupted", corrupted), symptomPart("unknown", unknown), symptomPart("invalid", invalid))
                .filter(Objects::nonNull)
                .collect(Collectors.joining(", and "))
            + ".";
    }

    private static String symptomPart(String type, int size) {
        if (size == 0) {
            return null;
        }
        return String.format(Locale.ROOT, "[%d] %s snapshot repositor%s", size, type, size > 1 ? "ies" : "y");
    }

    private RepositoryIntegrityHealthIndicatorService createRepositoryIntegrityHealthIndicatorService(ClusterState clusterState) {
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
