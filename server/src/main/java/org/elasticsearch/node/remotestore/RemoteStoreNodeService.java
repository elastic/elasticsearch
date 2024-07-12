/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.node.remotestore;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlags;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;

/**
 * Contains all the method needed for a remote store backed node lifecycle.
 */
public class RemoteStoreNodeService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreNodeService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;
    public static final Setting<CompatibilityMode> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = new Setting<>(
        "cluster.remote_store.compatibility_mode",
        CompatibilityMode.STRICT.name(),
        CompatibilityMode::parseString,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Direction> MIGRATION_DIRECTION_SETTING = new Setting<>(
        "cluster.migration.direction",
        Direction.NONE.name(),
        Direction::parseString,
        value -> {
            if (value == Direction.DOCREP && FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING) == false) {
                throw new IllegalArgumentException(
                    " remote store to docrep migration.direction is under an experimental feature and can be activated only by enabling "
                        + REMOTE_STORE_MIGRATION_EXPERIMENTAL
                        + " feature flag in the JVM options "
                );
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node join compatibility mode introduced with remote backed storage.
     *
     * @opensearch.internal
     */
    public enum CompatibilityMode {
        STRICT("strict"),
        MIXED("mixed");

        public final String mode;

        CompatibilityMode(String mode) {
            this.mode = mode;
        }

        public static CompatibilityMode parseString(String compatibilityMode) {
            try {
                return CompatibilityMode.valueOf(compatibilityMode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + compatibilityMode
                        + "] compatibility mode is not supported. "
                        + "supported modes are ["
                        + Arrays.toString(CompatibilityMode.values())
                        + "]"
                );
            }
        }
    }

    /**
     * Migration Direction intended for docrep to remote store migration and vice versa
     *
     * @opensearch.internal
     */
    public enum Direction {
        REMOTE_STORE("remote_store"),
        NONE("none"),
        DOCREP("docrep");

        public final String direction;

        Direction(String d) {
            this.direction = d;
        }

        public static Direction parseString(String direction) {
            try {
                return Direction.valueOf(direction.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[" + direction + "] migration.direction is not supported.");
            }
        }
    }

    public RemoteStoreNodeService(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    /**
     * Creates a repository during a node startup and performs verification by invoking verify method against
     * mentioned repository. This verification will happen on a local node to validate if the node is able to connect
     * to the repository with appropriate permissions.
     * If the creation or verification fails this will close all the repositories this method created and throw
     * exception.
     */
    public void createAndVerifyRepositories(DiscoveryNode localNode) {
        RemoteStoreNodeAttribute nodeAttribute = new RemoteStoreNodeAttribute(localNode);
        RepositoriesService reposService = repositoriesService.get();
        Map<String, Repository> repositories = new HashMap<>();
        for (RepositoryMetadata repositoryMetadata : nodeAttribute.getRepositoriesMetadata().repositories()) {
            String repositoryName = repositoryMetadata.name();
            Repository repository;
            RepositoriesService.validate(repositoryName);

            // Create Repository
            repository = reposService.createRepository(repositoryMetadata);
            logger.info(
                "remote backed storage repository with name [{}] and type [{}] created",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );

            // Verify Repository
            String verificationToken = repository.startVerification();
            repository.verify(verificationToken, localNode);
            repository.endVerification(verificationToken);
            logger.info(() -> String.valueOf(new ParameterizedMessage("successfully verified [{}] repository", repositoryName)));
            repositories.put(repositoryName, repository);
        }
        // Updating the repositories map in RepositoriesService
        reposService.updateRepositoriesMap(repositories);
    }

    /**
     * Updates repositories metadata in the cluster state if not already present. If a repository metadata for a
     * repository is already present in the cluster state and if it's different then the joining remote store backed
     * node repository metadata an exception will be thrown and the node will not be allowed to join the cluster.
     */
    public RepositoriesMetadata updateRepositoriesMetadata(DiscoveryNode joiningNode, RepositoriesMetadata existingRepositories) {
        if (joiningNode.isRemoteStoreNode()) {
            List<RepositoryMetadata> updatedRepositoryMetadataList = new ArrayList<>();
            List<RepositoryMetadata> newRepositoryMetadataList = new RemoteStoreNodeAttribute(joiningNode).getRepositoriesMetadata()
                .repositories();

            if (existingRepositories == null) {
                return new RepositoriesMetadata(newRepositoryMetadataList);
            } else {
                updatedRepositoryMetadataList.addAll(existingRepositories.repositories());
            }

            for (RepositoryMetadata newRepositoryMetadata : newRepositoryMetadataList) {
                boolean repositoryAlreadyPresent = false;
                for (RepositoryMetadata existingRepositoryMetadata : existingRepositories.repositories()) {
                    if (newRepositoryMetadata.name().equals(existingRepositoryMetadata.name())) {
                        try {
                            // This will help in handling two scenarios -
                            // 1. When a fresh cluster is formed and a node tries to join the cluster, the repository
                            // metadata constructed from the node attributes of the joining node will be validated
                            // against the repository information provided by existing nodes in cluster state.
                            // 2. It's possible to update repository settings except the restricted ones post the
                            // creation of a system repository and if a node drops we will need to allow it to join
                            // even if the non-restricted system repository settings are now different.
                            repositoriesService.get().ensureValidSystemRepositoryUpdate(newRepositoryMetadata, existingRepositoryMetadata);
                            newRepositoryMetadata = existingRepositoryMetadata;
                            repositoryAlreadyPresent = true;
                            break;
                        } catch (RepositoryException e) {
                            throw new IllegalStateException(
                                "new repository metadata ["
                                    + newRepositoryMetadata
                                    + "] supplied by joining node is different from existing repository metadata ["
                                    + existingRepositoryMetadata
                                    + "]."
                            );
                        }
                    }
                }
                if (repositoryAlreadyPresent == false) {
                    updatedRepositoryMetadataList.add(newRepositoryMetadata);
                }
            }
            return new RepositoriesMetadata(updatedRepositoryMetadataList);
        } else {
            return existingRepositories;
        }
    }

    /**
     * Returns <code>true</code> iff current cluster settings have:
     * <br>
     * - <code>remote_store.compatibility_mode</code> set to <code>mixed</code>
     * <br>
     * - <code>migration.direction</code> set to <code>remote_store</code>
     * <br>
     * <code>false</code> otherwise
     *
     * @param clusterSettings cluster level settings
     * @return
     * <code>true</code> For <code>REMOTE_STORE</code> migration direction and <code>MIXED</code> compatibility mode,
     * <code>false</code> otherwise
     */
    public static boolean isMigratingToRemoteStore(ClusterSettings clusterSettings) {
        boolean isMixedMode = clusterSettings.get(REMOTE_STORE_COMPATIBILITY_MODE_SETTING).equals(CompatibilityMode.MIXED);
        boolean isRemoteStoreMigrationDirection = clusterSettings.get(MIGRATION_DIRECTION_SETTING).equals(Direction.REMOTE_STORE);

        return (isMixedMode && isRemoteStoreMigrationDirection);
    }

    /**
     * To check if the cluster is undergoing remote store migration using clusterState metadata
     * @return
     * <code>true</code> For <code>REMOTE_STORE</code> migration direction and <code>MIXED</code> compatibility mode,
     * <code>false</code> otherwise
     */
    public static boolean isMigratingToRemoteStore(Metadata metadata) {
        boolean isMixedMode = REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(metadata.settings()).equals(CompatibilityMode.MIXED);
        boolean isRemoteStoreMigrationDirection = MIGRATION_DIRECTION_SETTING.get(metadata.settings()).equals(Direction.REMOTE_STORE);

        return (isMixedMode && isRemoteStoreMigrationDirection);
    }
}
