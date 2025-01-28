/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.RepositoriesHealthInfo;
import org.elasticsearch.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.elasticsearch.health.Diagnosis.Resource.Type.SNAPSHOT_REPOSITORY;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.UNKNOWN;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * This indicator reports health for snapshot repositories.
 *
 * Indicator will report RED status when any of snapshot repositories is marked as corrupted.
 * Data might not be backed up in such cases.
 *
 * Corrupted repository most likely need to be manually cleaned and a new snapshot needs to be created from scratch.
 */
public class RepositoryIntegrityHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "repository_integrity";

    private static final String HELP_URL = "https://ela.st/fix-repository-integrity";

    public static final String NO_REPOS_CONFIGURED = "No snapshot repositories configured.";
    public static final String ALL_REPOS_HEALTHY = "All repositories are healthy.";
    public static final String NO_REPO_HEALTH_INFO = "No repository health info.";
    public static final String MIXED_VERSIONS =
        "No repository health info. The cluster currently has mixed versions (an upgrade may be in progress).";

    public static final List<HealthIndicatorImpact> IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "backups_at_risk",
            2,
            "Data in the affected snapshot repositories may be lost and cannot be restored.",
            List.of(ImpactArea.BACKUP)
        )
    );
    public static final Diagnosis.Definition CORRUPTED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "corrupt_repo_integrity",
        "Multiple clusters are writing to the same repository.",
        "Remove the repository from the other cluster(s), or mark it as read-only in the other cluster(s), and then re-add the repository"
            + " to this cluster.",
        HELP_URL
    );
    public static final Diagnosis.Definition UNKNOWN_DEFINITION = new Diagnosis.Definition(
        NAME,
        "unknown_repository",
        "The repository uses an unknown type.",
        "Ensure that all required plugins are installed on the affected nodes.",
        HELP_URL
    );
    public static final Diagnosis.Definition INVALID_DEFINITION = new Diagnosis.Definition(
        NAME,
        "invalid_repository",
        "An exception occurred while trying to initialize the repository.",
        """
            Make sure all nodes in the cluster are in sync with each other.\
            Refer to the nodes’ logs for detailed information on why the repository initialization failed.""",
        HELP_URL
    );

    private final ClusterService clusterService;

    public RepositoryIntegrityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        var clusterState = clusterService.state();
        var snapshotMetadata = RepositoriesMetadata.get(clusterService.state());

        var repositories = snapshotMetadata.repositories();
        if (repositories.isEmpty()) {
            return createIndicator(GREEN, NO_REPOS_CONFIGURED, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        }

        var repositoryHealthAnalyzer = new RepositoryHealthAnalyzer(clusterState, repositories, healthInfo.repositoriesInfoByNode());
        return createIndicator(
            repositoryHealthAnalyzer.getHealthStatus(),
            repositoryHealthAnalyzer.getSymptom(),
            repositoryHealthAnalyzer.getDetails(verbose),
            repositoryHealthAnalyzer.getImpacts(),
            repositoryHealthAnalyzer.getDiagnoses(verbose, maxAffectedResourcesCount)
        );
    }

    /**
     * Analyzer for the cluster's repositories health; aids in constructing a {@link HealthIndicatorResult}.
     */
    class RepositoryHealthAnalyzer {
        private final ClusterState clusterState;
        private final int totalRepositories;
        private final List<String> corruptedRepositories;
        private final Set<String> unknownRepositories = new HashSet<>();
        private final Set<String> nodesWithUnknownRepos = new HashSet<>();
        private final Set<String> invalidRepositories = new HashSet<>();
        private final Set<String> nodesWithInvalidRepos = new HashSet<>();
        private final HealthStatus healthStatus;
        private boolean clusterHasFeature = true;

        private RepositoryHealthAnalyzer(
            ClusterState clusterState,
            List<RepositoryMetadata> repositories,
            Map<String, RepositoriesHealthInfo> repositoriesHealthByNode
        ) {
            this.clusterState = clusterState;
            this.totalRepositories = repositories.size();
            this.corruptedRepositories = repositories.stream()
                .filter(repository -> repository.generation() == RepositoryData.CORRUPTED_REPO_GEN)
                .map(RepositoryMetadata::name)
                .sorted()
                .toList();

            repositoriesHealthByNode.forEach((nodeId, healthInfo) -> {
                unknownRepositories.addAll(healthInfo.unknownRepositories());
                if (healthInfo.unknownRepositories().isEmpty() == false) {
                    nodesWithUnknownRepos.add(nodeId);
                }
                invalidRepositories.addAll(healthInfo.invalidRepositories());
                if (healthInfo.invalidRepositories().isEmpty() == false) {
                    nodesWithInvalidRepos.add(nodeId);
                }
            });

            if (corruptedRepositories.isEmpty() == false
                || unknownRepositories.isEmpty() == false
                || invalidRepositories.isEmpty() == false) {
                healthStatus = YELLOW;
            } else if (repositoriesHealthByNode.isEmpty()) {
                clusterHasFeature = false;
                healthStatus = UNKNOWN;
            } else {
                healthStatus = GREEN;
            }
        }

        public HealthStatus getHealthStatus() {
            return healthStatus;
        }

        public String getSymptom() {
            if (healthStatus == GREEN) {
                return clusterHasFeature ? ALL_REPOS_HEALTHY : MIXED_VERSIONS;
            } else if (healthStatus == UNKNOWN) {
                return NO_REPO_HEALTH_INFO;
            }

            return "Detected "
                + Stream.of(
                    generateSymptomString("corrupted", corruptedRepositories.size()),
                    generateSymptomString("unknown", unknownRepositories.size()),
                    generateSymptomString("invalid", invalidRepositories.size())
                ).filter(Objects::nonNull).collect(Collectors.joining(", and "))
                + ".";
        }

        private static String generateSymptomString(String type, long size) {
            if (size == 0) {
                return null;
            }

            return String.format(Locale.ROOT, "[%d] %s snapshot repositor%s", size, type, size > 1 ? "ies" : "y");
        }

        public HealthIndicatorDetails getDetails(boolean verbose) {
            if (verbose == false) {
                return HealthIndicatorDetails.EMPTY;
            }
            Map<String, Object> map = new HashMap<>();
            map.put("total_repositories", totalRepositories);

            if (healthStatus != GREEN) {
                map.put("corrupted_repositories", corruptedRepositories.size());
                map.put("corrupted", limitSize(corruptedRepositories, 10));

                if (healthStatus != UNKNOWN) {
                    map.put("unknown_repositories", unknownRepositories.size());
                    map.put("invalid_repositories", invalidRepositories.size());
                }
            }

            return new SimpleHealthIndicatorDetails(map);
        }

        public List<HealthIndicatorImpact> getImpacts() {
            if (healthStatus == GREEN || healthStatus == UNKNOWN) {
                return List.of();
            }
            return IMPACTS;
        }

        public List<Diagnosis> getDiagnoses(boolean verbose, int maxAffectedResourcesCount) {
            if (verbose == false) {
                return List.of();
            }
            var diagnoses = new ArrayList<Diagnosis>();
            if (corruptedRepositories.isEmpty() == false) {
                diagnoses.add(
                    new Diagnosis(
                        CORRUPTED_DEFINITION,
                        List.of(new Diagnosis.Resource(SNAPSHOT_REPOSITORY, limitSize(corruptedRepositories, maxAffectedResourcesCount)))
                    )
                );
            }
            if (unknownRepositories.isEmpty() == false) {
                diagnoses.add(createDiagnosis(UNKNOWN_DEFINITION, unknownRepositories, nodesWithUnknownRepos, maxAffectedResourcesCount));
            }
            if (invalidRepositories.isEmpty() == false) {
                diagnoses.add(createDiagnosis(INVALID_DEFINITION, invalidRepositories, nodesWithInvalidRepos, maxAffectedResourcesCount));
            }
            return diagnoses;
        }

        private Diagnosis createDiagnosis(
            Diagnosis.Definition definition,
            Set<String> repos,
            Set<String> nodes,
            int maxAffectedResourcesCount
        ) {
            var reposView = repos.stream().sorted().limit(maxAffectedResourcesCount).toList();
            var nodesView = nodes.stream()
                .map(nodeId -> clusterState.nodes().get(nodeId))
                .sorted(DISCOVERY_NODE_COMPARATOR)
                .limit(maxAffectedResourcesCount)
                .toList();
            return new Diagnosis(
                definition,
                List.of(new Diagnosis.Resource(SNAPSHOT_REPOSITORY, reposView), new Diagnosis.Resource(nodesView))
            );
        }
    }
}
