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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.HealthIndicatorDisplayValues;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
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
    private boolean multiProject;
    private Set<ProjectId> projectIds;
    private ProjectResolver projectResolver;

    @Before
    public void initNodes() {
        node1 = DiscoveryNodeUtils.create(randomAlphaOfLength(10), randomUUID());
        node2 = DiscoveryNodeUtils.create(randomAlphaOfLength(10), randomUUID());
        multiProject = randomBoolean();
        projectIds = multiProject
            ? IntStream.range(0, randomIntBetween(1, 5)).mapToObj(i -> randomUniqueProjectId()).collect(Collectors.toSet())
            : Set.of(randomProjectIdOrDefault());
        projectResolver = multiProject
            ? TestProjectResolvers.allProjects()
            : TestProjectResolvers.singleProjectOnly(projectIds.iterator().next());
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
                    new SimpleHealthIndicatorDetails(Map.of("total_repositories", repos.size() * projectIds.size())),
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

        List<String> corruptedNames = displayNames(corruptedRepos);
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), 0, 0),
                    createDetails(repos.size() * projectIds.size(), corruptedNames.size(), corruptedNames, 0, 0),
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
        List<String> unknownNames = displayNames(unknownRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(unknownNames, List.of()));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(0, unknownNames.size(), 0),
                    createDetails(repos.size() * projectIds.size(), 0, List.of(), unknownNames.size(), 0),
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
        List<String> invalidNames = displayNames(invalidRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of(), invalidNames));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(0, 0, invalidNames.size()),
                    createDetails(repos.size() * projectIds.size(), 0, List.of(), 0, invalidNames.size()),
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
        List<String> corruptedNames = displayNames(corruptedRepos);
        List<String> unknownNames = displayNames(unknownRepos);
        List<String> invalidNames = displayNames(invalidRepos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(unknownNames, List.of()));
        healthInfo.repositoriesInfoByNode().put(node2.getId(), new RepositoriesHealthInfo(List.of(), invalidNames));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), unknownNames.size(), invalidNames.size()),
                    createDetails(
                        repos.size() * projectIds.size(),
                        corruptedNames.size(),
                        corruptedNames,
                        unknownNames.size(),
                        invalidNames.size()
                    ),
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
        var clusterState = createClusterStateWith(new RepositoriesMetadata(List.of()));
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
                            Map.of(
                                "total_repositories",
                                repos.size() * projectIds.size(),
                                "corrupted_repositories",
                                0,
                                "corrupted",
                                List.of()
                            )
                        )
                        : HealthIndicatorDetails.EMPTY,
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    /**
     * If repositories have already been marked as corrupted, the result should stay yellow even when nodes have not
     * reported any repository health yet.
     */
    public void testIsYellowWhenCorruptedReposExistAndNoHealthInfoIsAvailable() {
        var corruptedRepos = createNamedRepositories("corrupted-repo-", true);
        var repos = concatLists(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)), corruptedRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        var verbose = randomBoolean();
        List<String> corruptedNames = displayNames(corruptedRepos);

        assertThat(
            service.calculate(verbose, new HealthInfo(Map.of(), null, Map.of(), FileSettingsHealthInfo.INDETERMINATE)),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), 0, 0),
                    verbose
                        ? createDetails(repos.size() * projectIds.size(), corruptedNames.size(), corruptedNames, 0, 0)
                        : HealthIndicatorDetails.EMPTY,
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    verbose
                        ? List.of(
                            new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedNames)))
                        )
                        : List.of()
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
            unknownRepos.addAll(displayNames("unknown-repo-" + i));
            invalidRepos.addAll(displayNames("invalid-repo-" + i));
            repoHealthInfo.put(
                "node-" + i,
                new RepositoriesHealthInfo(displayNames("unknown-repo-" + i), displayNames("invalid-repo-" + i))
            );
        });
        healthInfo = new HealthInfo(
            healthInfo.diskInfoByNode(),
            healthInfo.dslHealthInfo(),
            repoHealthInfo,
            FileSettingsHealthInfo.INDETERMINATE
        );

        List<String> expectedCorruptedDetails = limitSize(displayNames(repos), 10);
        var expectedDetails = createDetails(
            repos.size() * projectIds.size(),
            repos.size() * projectIds.size(),
            expectedCorruptedDetails,
            unknownRepos.size(),
            invalidRepos.size()
        );

        var resultLimitedToTen = service.calculate(true, 10, healthInfo);
        assertThat(resultLimitedToTen.diagnosisList(), equalTo(createDiagnoses(repos, nodes, unknownRepos, invalidRepos, 10)));
        assertThat(resultLimitedToTen.details(), equalTo(expectedDetails));

        var resultLimitedToZero = service.calculate(true, 0, healthInfo);
        assertThat(resultLimitedToZero.diagnosisList(), equalTo(createDiagnoses(repos, nodes, unknownRepos, invalidRepos, 0)));
        assertThat(resultLimitedToZero.details(), equalTo(expectedDetails));
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        int problemType = randomIntBetween(0, 2);
        boolean corrupted = problemType == 0;
        boolean unknown = problemType == 1;
        var problemRepos = createNamedRepositories(corrupted ? "corrupted-repo-" : unknown ? "unknown-repo-" : "invalid-repo-", corrupted);
        var repos = concatLists(randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false)), problemRepos);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        List<String> problemNames = displayNames(problemRepos);
        if (unknown) {
            healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(problemNames, List.of()));
        } else if (corrupted == false) {
            healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of(), problemNames));
        }

        assertThat(
            service.calculate(false, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(
                        corrupted ? problemNames.size() : 0,
                        unknown ? problemNames.size() : 0,
                        (corrupted || unknown) ? 0 : problemNames.size()
                    ),
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
        List<String> displayedRepoNames = displayNames(repoName);
        var clusterState = createClusterStateWith(new RepositoriesMetadata(List.of(createRepositoryMetadata(repoName, false))));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState);
        var repoHealth = unknown
            ? new RepositoriesHealthInfo(displayedRepoNames, List.of())
            : new RepositoriesHealthInfo(List.of(), displayedRepoNames);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), repoHealth);
        healthInfo.repositoriesInfoByNode().put(node2.getId(), repoHealth);

        int repoCount = displayedRepoNames.size();
        List<DiscoveryNode> expectedNodes = Stream.of(node1, node2).sorted(DISCOVERY_NODE_COMPARATOR).toList();
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(0, unknown ? repoCount : 0, unknown ? 0 : repoCount),
                    createDetails(repoCount, 0, List.of(), unknown ? repoCount : 0, unknown ? 0 : repoCount),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    List.of(
                        new Diagnosis(
                            unknown ? UNKNOWN_DEFINITION : INVALID_DEFINITION,
                            List.of(
                                new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, displayedRepoNames),
                                new Diagnosis.Resource(expectedNodes)
                            )
                        )
                    )
                )
            )
        );
    }

    /**
     * If one project is fine and another has a bad snapshot repository, the cluster should report yellow and
     * only name the bad repos.
     */
    public void testIsYellowWhenOneProjectIsHealthyAndAnotherIsUnhealthy() {
        var healthyProject = randomUniqueProjectId();
        var unhealthyProject = randomUniqueProjectId();
        var healthyRepos = randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        int problemType = randomIntBetween(0, 2);
        boolean corrupted = problemType == 0;
        boolean unknown = problemType == 1;
        var problemRepos = createNamedRepositories(corrupted ? "corrupted-repo-" : unknown ? "unknown-repo-" : "invalid-repo-", corrupted);
        var clusterState = createClusterStateWith(Map.of(healthyProject, healthyRepos, unhealthyProject, problemRepos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState, TestProjectResolvers.allProjects());
        List<String> problemNames = displayNames(unhealthyProject, problemRepos);
        if (unknown) {
            healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(problemNames, List.of()));
        } else if (corrupted == false) {
            healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(List.of(), problemNames));
        }

        List<Diagnosis> expectedDiagnoses = corrupted
            ? List.of(new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, problemNames))))
            : List.of(
                new Diagnosis(
                    unknown ? UNKNOWN_DEFINITION : INVALID_DEFINITION,
                    List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, problemNames), new Diagnosis.Resource(List.of(node1)))
                )
            );
        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(
                        corrupted ? problemNames.size() : 0,
                        unknown ? problemNames.size() : 0,
                        (corrupted || unknown) ? 0 : problemNames.size()
                    ),
                    createDetails(
                        healthyRepos.size() + problemRepos.size(),
                        corrupted ? problemNames.size() : 0,
                        corrupted ? problemNames : List.of(),
                        unknown ? problemNames.size() : 0,
                        (corrupted || unknown) ? 0 : problemNames.size()
                    ),
                    RepositoryIntegrityHealthIndicatorService.IMPACTS,
                    expectedDiagnoses
                )
            )
        );
    }

    /**
     * An empty project should not make the cluster look like it has no repositories when another project still has some.
     */
    public void testEmptyAndNonEmptyProjects() {
        var emptyProject = randomUniqueProjectId();
        var populatedProject = randomUniqueProjectId();
        boolean unhealthy = randomBoolean();
        var populatedRepos = unhealthy
            ? createNamedRepositories("corrupted-repo-", true)
            : randomList(1, 10, () -> createRepositoryMetadata("healthy-repo", false));
        var clusterState = createClusterStateWith(Map.of(emptyProject, List.of(), populatedProject, populatedRepos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState, TestProjectResolvers.allProjects());

        if (unhealthy) {
            List<String> corruptedNames = displayNames(populatedProject, populatedRepos);
            assertThat(
                service.calculate(true, healthInfo),
                equalTo(
                    new HealthIndicatorResult(
                        NAME,
                        YELLOW,
                        expectedSymptom(corruptedNames.size(), 0, 0),
                        createDetails(populatedRepos.size(), corruptedNames.size(), corruptedNames, 0, 0),
                        RepositoryIntegrityHealthIndicatorService.IMPACTS,
                        List.of(
                            new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, corruptedNames)))
                        )
                    )
                )
            );
        } else {
            assertThat(
                service.calculate(true, healthInfo),
                equalTo(
                    new HealthIndicatorResult(
                        NAME,
                        GREEN,
                        RepositoryIntegrityHealthIndicatorService.ALL_REPOS_HEALTHY,
                        new SimpleHealthIndicatorDetails(Map.of("total_repositories", populatedRepos.size())),
                        Collections.emptyList(),
                        Collections.emptyList()
                    )
                )
            );
        }
    }

    /**
     * Problems of different kinds in different projects are added together even when those projects use the same repository names.
     */
    public void testYellowProjectsAreAggregated() {
        var corruptedProject = randomUniqueProjectId();
        var unknownProject = randomUniqueProjectId();
        var invalidProject = randomUniqueProjectId();
        var repos = createNamedRepositories("repo-", false);
        var corruptedRepos = repos.stream().map(repository -> createRepositoryMetadata(repository.name(), true)).toList();
        var clusterState = createClusterStateWith(Map.of(corruptedProject, corruptedRepos, unknownProject, repos, invalidProject, repos));
        var service = createRepositoryIntegrityHealthIndicatorService(clusterState, TestProjectResolvers.allProjects());
        List<String> corruptedNames = displayNames(corruptedProject, corruptedRepos);
        List<String> unknownNames = displayNames(unknownProject, repos);
        List<String> invalidNames = displayNames(invalidProject, repos);
        healthInfo.repositoriesInfoByNode().put(node1.getId(), new RepositoriesHealthInfo(unknownNames, List.of()));
        healthInfo.repositoriesInfoByNode().put(node2.getId(), new RepositoriesHealthInfo(List.of(), invalidNames));

        assertThat(
            service.calculate(true, healthInfo),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    expectedSymptom(corruptedNames.size(), unknownNames.size(), invalidNames.size()),
                    createDetails(
                        corruptedRepos.size() + repos.size() + repos.size(),
                        corruptedNames.size(),
                        corruptedNames,
                        unknownNames.size(),
                        invalidNames.size()
                    ),
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
                    new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, displayNames(repos).stream().limit(maxAffectedResourcesCount).toList())
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
        Map<ProjectId, List<RepositoryMetadata>> repositoriesByProject = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            repositoriesByProject.put(projectId, metadata == null ? List.of() : metadata.repositories());
        }
        return createClusterStateWith(repositoriesByProject);
    }

    private ClusterState createClusterStateWith(Map<ProjectId, List<RepositoryMetadata>> repositoriesByProject) {
        var builder = ClusterState.builder(new ClusterName("test-cluster")).nodes(DiscoveryNodes.builder().add(node1).add(node2).build());
        var metadataBuilder = Metadata.builder();
        repositoriesByProject.forEach((projectId, repos) -> {
            var projectBuilder = ProjectMetadata.builder(projectId);
            if (repos != null) {
                projectBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repos));
            }
            metadataBuilder.put(projectBuilder);
        });
        return builder.metadata(metadataBuilder).build();
    }

    private static RepositoryMetadata createRepositoryMetadata(String name, boolean corrupted) {
        return new RepositoryMetadata(name, "uuid", "s3", Settings.EMPTY, corrupted ? CORRUPTED_REPO_GEN : EMPTY_REPO_GEN, EMPTY_REPO_GEN);
    }

    private static List<RepositoryMetadata> createNamedRepositories(String namePrefix, boolean corrupted) {
        return IntStream.range(0, randomIntBetween(1, 10)).mapToObj(i -> createRepositoryMetadata(namePrefix + i, corrupted)).toList();
    }

    private List<String> displayNames(String repositoryName) {
        return projectIds.stream()
            .map(projectId -> HealthIndicatorDisplayValues.getRepositoryDisplayName(projectId, repositoryName, multiProject))
            .sorted()
            .toList();
    }

    private List<String> displayNames(List<RepositoryMetadata> repos) {
        return projectIds.stream()
            .flatMap(
                projectId -> repos.stream()
                    .map(repository -> HealthIndicatorDisplayValues.getRepositoryDisplayName(projectId, repository.name(), multiProject))
            )
            .sorted()
            .toList();
    }

    private static List<String> displayNames(ProjectId projectId, List<RepositoryMetadata> repos) {
        return repos.stream()
            .map(repository -> HealthIndicatorDisplayValues.getRepositoryDisplayName(projectId, repository.name(), true))
            .sorted()
            .toList();
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
        return createRepositoryIntegrityHealthIndicatorService(clusterState, projectResolver);
    }

    private RepositoryIntegrityHealthIndicatorService createRepositoryIntegrityHealthIndicatorService(
        ClusterState clusterState,
        ProjectResolver resolver
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new RepositoryIntegrityHealthIndicatorService(clusterService, resolver);
    }

    private SimpleHealthIndicatorDetails createDetails(int total, int corruptedCount, List<String> corrupted, int unknown, int invalid) {
        return new SimpleHealthIndicatorDetails(
            Map.of(
                "total_repositories",
                total,
                "corrupted_repositories",
                corruptedCount,
                "corrupted",
                limitSize(corrupted, 10),
                "unknown_repositories",
                unknown,
                "invalid_repositories",
                invalid
            )
        );
    }
}
