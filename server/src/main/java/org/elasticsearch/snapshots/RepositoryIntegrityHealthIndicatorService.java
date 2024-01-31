/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static final String HELP_URL = "https://ela.st/fix-repository-integrity";

    public static final String NO_REPOS_CONFIGURED = "No snapshot repositories configured.";
    public static final String ALL_REPOS_HEALTHY = "All repositories are healthy.";

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
        if (healthInfo.repositoriesInfoByNode().isEmpty()) {
            return createIndicator(UNKNOWN, "No repository health info.", HealthIndicatorDetails.EMPTY, List.of(), List.of());
        }
        var clusterState = clusterService.state();
        var snapshotMetadata = RepositoriesMetadata.get(clusterService.state());
        var totalRepositories = snapshotMetadata.repositories().size();
        var corrupted = snapshotMetadata.repositories()
            .stream()
            .filter(repository -> repository.generation() == RepositoryData.CORRUPTED_REPO_GEN)
            .map(RepositoryMetadata::name)
            .toList();

        var repositoryHealthAnalyzer = new RepositoryHealthAnalyzer(
            clusterState,
            totalRepositories,
            corrupted,
            healthInfo.repositoriesInfoByNode()
        );
        return createIndicator(
            repositoryHealthAnalyzer.getHealthStatus(),
            repositoryHealthAnalyzer.getSymptom(),
            repositoryHealthAnalyzer.getDetails(verbose),
            repositoryHealthAnalyzer.getImpacts(),
            repositoryHealthAnalyzer.getDiagnoses(maxAffectedResourcesCount)
        );
    }

    /**
     * Analyzer for the cluster's repositories health; aids in constructing a {@link HealthIndicatorResult}.
     */
    static class RepositoryHealthAnalyzer {
        private final ClusterState clusterState;
        private final int totalRepositories;
        private final List<String> corruptedRepositories;
        private final Map<String, List<String>> unknownRepositories;
        private final Map<String, List<String>> invalidRepositories;
        private final int unknownRepositoriesCount;
        private final int invalidRepositoriesCount;
        private final HealthStatus healthStatus;

        private RepositoryHealthAnalyzer(
            ClusterState clusterState,
            int totalRepositories,
            List<String> corruptedRepositories,
            Map<String, RepositoriesHealthInfo> repositoriesHealthByNode
        ) {
            this.clusterState = clusterState;
            this.totalRepositories = totalRepositories;
            this.corruptedRepositories = corruptedRepositories;
            this.unknownRepositories = new HashMap<>();
            this.invalidRepositories = new HashMap<>();

            var unknownRepositoriesCount = new AtomicInteger();
            var invalidRepositoriesCount = new AtomicInteger();
            repositoriesHealthByNode.forEach((nodeId, healthInfo) -> {
                unknownRepositories.computeIfAbsent(nodeId, k -> new ArrayList<>()).addAll(healthInfo.unknownRepositories());
                invalidRepositories.computeIfAbsent(nodeId, k -> new ArrayList<>()).addAll(healthInfo.invalidRepositories());

                unknownRepositoriesCount.getAndAdd(healthInfo.unknownRepositories().size());
                invalidRepositoriesCount.getAndAdd(healthInfo.invalidRepositories().size());
            });
            this.unknownRepositoriesCount = unknownRepositoriesCount.get();
            this.invalidRepositoriesCount = invalidRepositoriesCount.get();

            healthStatus = corruptedRepositories.isEmpty() && unknownRepositoriesCount.get() == 0 && invalidRepositoriesCount.get() == 0
                ? GREEN
                : YELLOW;
        }

        public HealthStatus getHealthStatus() {
            return healthStatus;
        }

        public String getSymptom() {
            if (totalRepositories == 0) {
                return NO_REPOS_CONFIGURED;
            }
            if (healthStatus == GREEN) {
                return ALL_REPOS_HEALTHY;
            }

            return "Detected "
                + Stream.of(
                    generateSymptomString("corrupted", corruptedRepositories.size()),
                    generateSymptomString("unknown", unknownRepositoriesCount),
                    generateSymptomString("invalid", invalidRepositoriesCount)
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

            if (healthStatus == GREEN) {
                return new SimpleHealthIndicatorDetails(Map.of("total_repositories", totalRepositories));
            }

            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "total_repositories",
                    totalRepositories,
                    "corrupted_repositories",
                    corruptedRepositories.size(),
                    "corrupted",
                    limitSize(corruptedRepositories, 10),
                    "unknown_repositories",
                    unknownRepositoriesCount,
                    "invalid_repositories",
                    invalidRepositoriesCount
                )
            );
        }

        public List<HealthIndicatorImpact> getImpacts() {
            if (healthStatus == GREEN) {
                return List.of();
            }
            return IMPACTS;
        }

        public List<Diagnosis> getDiagnoses(int maxAffectedResourcesCount) {
            var diagnoses = new ArrayList<Diagnosis>();
            if (corruptedRepositories.isEmpty() == false) {
                diagnoses.add(
                    new Diagnosis(
                        CORRUPTED_DEFINITION,
                        List.of(new Diagnosis.Resource(SNAPSHOT_REPOSITORY, limitSize(corruptedRepositories, maxAffectedResourcesCount)))
                    )
                );
            }
            if (unknownRepositoriesCount > 0) {
                diagnoses.add(createDiagnosis(UNKNOWN_DEFINITION, unknownRepositories, maxAffectedResourcesCount));
            }
            if (invalidRepositoriesCount > 0) {
                diagnoses.add(createDiagnosis(INVALID_DEFINITION, invalidRepositories, maxAffectedResourcesCount));
            }
            return diagnoses;
        }

        private Diagnosis createDiagnosis(
            Diagnosis.Definition definition,
            Map<String, List<String>> reposMap,
            int maxAffectedResourcesCount
        ) {
            // Flat map all the repo's (and make sure we only return distinct repo names).
            var repoNames = reposMap.values().stream().flatMap(Collection::stream).distinct().limit(maxAffectedResourcesCount).toList();
            var nodes = reposMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().isEmpty() == false)
                .map(entry -> clusterState.nodes().get(entry.getKey()))
                .limit(maxAffectedResourcesCount)
                .toList();
            return new Diagnosis(
                definition,
                List.of(new Diagnosis.Resource(SNAPSHOT_REPOSITORY, repoNames), new Diagnosis.Resource(nodes))
            );
        }
    }
}
