/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.security.SecurityFeatures;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;

/**
 * Manages the lifecycle, mapping and data upgrades/migrations of the {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS}
 * and {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS} alias-index pair.
 */
public class SecurityIndexManager implements ClusterStateListener {

    public static final String SECURITY_VERSION_STRING = "security-version";
    protected static final String FILE_SETTINGS_METADATA_NAMESPACE = "file_settings";
    private static final Logger logger = LogManager.getLogger(SecurityIndexManager.class);

    /**
     * When checking availability, check for availability of search or availability of all primaries
     **/
    public enum Availability {
        SEARCH_SHARDS,
        PRIMARY_SHARDS
    }

    public enum RoleMappingsCleanupMigrationStatus {
        READY,
        NOT_READY,
        SKIP,
        DONE
    }

    private final Client client;
    private final SystemIndexDescriptor systemIndexDescriptor;

    private final List<TriConsumer<ProjectId, IndexState, IndexState>> stateChangeListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<SecurityIndexManager>> stateRecoveredListeners = new CopyOnWriteArrayList<>();

    private volatile Map<ProjectId, IndexState> stateByProject;
    private final FeatureService featureService;
    private final ProjectResolver projectResolver;

    private final Set<NodeFeature> allSecurityFeatures = new SecurityFeatures().getFeatures();

    public static SecurityIndexManager buildSecurityIndexManager(
        Client client,
        ClusterService clusterService,
        FeatureService featureService,
        ProjectResolver projectResolver,
        SystemIndexDescriptor descriptor
    ) {
        final SecurityIndexManager securityIndexManager = new SecurityIndexManager(featureService, projectResolver, client, descriptor);
        clusterService.addListener(securityIndexManager);
        return securityIndexManager;
    }

    private SecurityIndexManager(
        FeatureService featureService,
        ProjectResolver projectResolver,
        Client client,
        SystemIndexDescriptor descriptor
    ) {
        this.featureService = featureService;
        this.projectResolver = projectResolver;
        this.client = client;
        this.systemIndexDescriptor = descriptor;
        this.stateByProject = null;
    }

    public enum ProjectStatus {
        CLUSTER_NOT_RECOVERED,
        PROJECT_DOES_NOT_EXIST,
        PROJECT_AVAILABLE
    }

    private IndexState unavailableState(ProjectId projectId, ProjectStatus status) {
        if (status == ProjectStatus.PROJECT_AVAILABLE) {
            throw new IllegalArgumentException("Unavailable status cannot be [" + status + "]");
        }
        return new IndexState(
            projectId,
            status,
            null,
            false,
            false,
            false,
            false,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Set.of()
        );
    }

    public class IndexState {
        private final ProjectId projectId;
        private final ProjectStatus projectStatus;

        public final Instant creationTime;
        public final boolean isIndexUpToDate;
        public final boolean indexAvailableForSearch;
        public final boolean indexAvailableForWrite;
        public final boolean mappingUpToDate;
        public final boolean createdOnLatestVersion;
        public final RoleMappingsCleanupMigrationStatus roleMappingsCleanupMigrationStatus;
        public final Integer migrationsVersion;
        // Min mapping version supported by the descriptors in the cluster
        public final SystemIndexDescriptor.MappingsVersion minClusterMappingVersion;
        // Applied mapping version
        public final Integer indexMappingVersion;
        public final String concreteIndexName;
        public final ClusterHealthStatus indexHealth;
        public final IndexMetadata.State indexState;
        public final String indexUUID;
        public final Set<NodeFeature> securityFeatures;

        public IndexState(
            ProjectId projectId,
            ProjectStatus projectStatus,
            Instant creationTime,
            boolean isIndexUpToDate,
            boolean indexAvailableForSearch,
            boolean indexAvailableForWrite,
            boolean mappingUpToDate,
            boolean createdOnLatestVersion,
            RoleMappingsCleanupMigrationStatus roleMappingsCleanupMigrationStatus,
            Integer migrationsVersion,
            SystemIndexDescriptor.MappingsVersion minClusterMappingVersion,
            Integer indexMappingVersion,
            String concreteIndexName,
            ClusterHealthStatus indexHealth,
            IndexMetadata.State indexState,
            String indexUUID,
            Set<NodeFeature> securityFeatures
        ) {
            this.projectId = projectId;
            this.projectStatus = projectStatus;
            this.creationTime = creationTime;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailableForSearch = indexAvailableForSearch;
            this.indexAvailableForWrite = indexAvailableForWrite;
            this.mappingUpToDate = mappingUpToDate;
            this.migrationsVersion = migrationsVersion;
            this.createdOnLatestVersion = createdOnLatestVersion;
            this.roleMappingsCleanupMigrationStatus = roleMappingsCleanupMigrationStatus;
            this.minClusterMappingVersion = minClusterMappingVersion;
            this.indexMappingVersion = indexMappingVersion;
            this.concreteIndexName = concreteIndexName;
            this.indexHealth = indexHealth;
            this.indexState = indexState;
            this.indexUUID = indexUUID;
            this.securityFeatures = securityFeatures;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexState other = (IndexState) o;
            return Objects.equals(projectId, other.projectId)
                && Objects.equals(projectStatus, other.projectStatus)
                && Objects.equals(creationTime, other.creationTime)
                && isIndexUpToDate == other.isIndexUpToDate
                && indexAvailableForSearch == other.indexAvailableForSearch
                && indexAvailableForWrite == other.indexAvailableForWrite
                && mappingUpToDate == other.mappingUpToDate
                && createdOnLatestVersion == other.createdOnLatestVersion
                && roleMappingsCleanupMigrationStatus == other.roleMappingsCleanupMigrationStatus
                && Objects.equals(indexMappingVersion, other.indexMappingVersion)
                && Objects.equals(migrationsVersion, other.migrationsVersion)
                && Objects.equals(minClusterMappingVersion, other.minClusterMappingVersion)
                && Objects.equals(concreteIndexName, other.concreteIndexName)
                && indexHealth == other.indexHealth
                && indexState == other.indexState
                && Objects.equals(securityFeatures, other.securityFeatures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                projectId,
                projectStatus,
                creationTime,
                isIndexUpToDate,
                indexAvailableForSearch,
                indexAvailableForWrite,
                mappingUpToDate,
                createdOnLatestVersion,
                roleMappingsCleanupMigrationStatus,
                migrationsVersion,
                minClusterMappingVersion,
                indexMappingVersion,
                concreteIndexName,
                indexHealth,
                securityFeatures
            );
        }

        public String aliasName() {
            return SecurityIndexManager.this.aliasName();
        }

        public boolean indexExists() {
            return creationTime != null;
        }

        public boolean indexIsClosed() {
            return this.indexState == IndexMetadata.State.CLOSE;
        }

        public Instant getCreationTime() {
            return this.creationTime;
        }

        /**
         * Returns whether the index is on the current format if it exists. If the index does not exist
         * we treat the index as up to date as we expect it to be created with the current format.
         */
        public boolean isIndexUpToDate() {
            return this.isIndexUpToDate;
        }

        /**
         * Optimization to avoid making unnecessary calls when we know the underlying shard state.
         * This call will check that the index exists,  is discoverable from the alias, is not closed, and will determine if available
         * based on the {@link Availability} parameter.
         * @param availability Check availability for search or write/update/real time get workflows. Write/update/realtime get workflows
         *                     should check for availability of primary shards. Search workflows should check availability of search shards
         *                     (which may or may not also be the primary shards).
         * @return
         * when checking for search: <code>true</code> if all searchable shards for the security index are available
         * when checking for primary: <code>true</code> if all primary shards for the security index are available
         */
        public boolean isAvailable(Availability availability) {
            switch (availability) {
                case SEARCH_SHARDS -> {
                    return this.indexAvailableForSearch;
                }
                case PRIMARY_SHARDS -> {
                    return this.indexAvailableForWrite;
                }
            }
            // can never happen
            throw new IllegalStateException("Unexpected availability enumeration. This is bug, please contact support.");
        }

        public boolean isMappingUpToDate() {
            return this.mappingUpToDate;
        }

        boolean isProjectAvailable() {
            return this.projectStatus == ProjectStatus.PROJECT_AVAILABLE;
        }

        // Available for testing only
        boolean isClusterStateRecovered() {
            return this.projectStatus != ProjectStatus.CLUSTER_NOT_RECOVERED;
        }

        public boolean isMigrationsVersionAtLeast(Integer expectedMigrationsVersion) {
            return indexExists() && this.migrationsVersion.compareTo(expectedMigrationsVersion) >= 0;
        }

        public boolean isCreatedOnLatestVersion() {
            return this.createdOnLatestVersion;
        }

        public String getConcreteIndexName() {
            return this.concreteIndexName;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + '{'
                + "projectId="
                + projectId
                + ", creationTime="
                + creationTime
                + ", isIndexUpToDate="
                + isIndexUpToDate
                + ", indexAvailableForSearch="
                + indexAvailableForSearch
                + ", indexAvailableForWrite="
                + indexAvailableForWrite
                + ", mappingUpToDate="
                + mappingUpToDate
                + ", createdOnLatestVersion="
                + createdOnLatestVersion
                + ", roleMappingsCleanupMigrationStatus="
                + roleMappingsCleanupMigrationStatus
                + ", migrationsVersion="
                + migrationsVersion
                + ", minClusterMappingVersion="
                + minClusterMappingVersion
                + ", indexMappingVersion="
                + indexMappingVersion
                + ", concreteIndexName='"
                + concreteIndexName
                + "', indexHealth="
                + indexHealth
                + ", indexState="
                + indexState
                + ", indexUUID='"
                + indexUUID
                + "', securityFeatures="
                + securityFeatures
                + '}';
        }

        public ElasticsearchException getUnavailableReason(Availability availability) {
            if (this.indexState == IndexMetadata.State.CLOSE) {
                return new IndexClosedException(new Index(this.concreteIndexName, ClusterState.UNKNOWN_UUID));
            } else if (this.indexExists()) {
                assert this.indexAvailableForSearch == false || this.indexAvailableForWrite == false;
                if (Availability.PRIMARY_SHARDS.equals(availability) && this.indexAvailableForWrite == false) {
                    return new UnavailableShardsException(
                        null,
                        "at least one primary shard for the index [" + this.concreteIndexName + "] is unavailable"
                    );
                } else if (Availability.SEARCH_SHARDS.equals(availability) && this.indexAvailableForSearch == false) {
                    // The current behavior is that when primaries are unavailable and replicas can not be promoted then
                    // any replicas will be marked as unavailable as well. This is applicable in stateless where there index only primaries
                    // with non-promotable replicas (i.e. search only shards). In the case "at least one search ... is unavailable" is
                    // a technically correct statement, but it may be unavailable because it is not promotable and the primary is
                    // unavailable
                    return new UnavailableShardsException(
                        null,
                        "at least one search shard for the index [" + this.concreteIndexName + "] is unavailable"
                    );
                } else {
                    // should never happen
                    throw new IllegalStateException("caller must ensure original availability matches the current availability");
                }
            } else {
                return new IndexNotFoundException(this.concreteIndexName);
            }
        }

        /**
         * Validates that the index is up to date and does not need to be migrated. If it is not, the
         * consumer is called with an exception. If the index is up to date, the runnable will
         * be executed. <b>NOTE:</b> this method does not check the availability of the index; this check
         * is left to the caller so that this condition can be handled appropriately.
         */
        public void checkIndexVersionThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
            if (this.indexExists() && this.isIndexUpToDate == false) {
                consumer.accept(
                    new IllegalStateException(
                        "Index ["
                            + this.concreteIndexName
                            + "] is not on the current version. Security features relying on the index"
                            + " will not be available until the upgrade API is run on the index"
                    )
                );
            } else {
                andThen.run();
            }
        }

        /**
         * Prepares the index by creating it if it doesn't exist, then executes the runnable.
         * @param consumer a handler for any exceptions that are raised either during preparation or execution
         * @param andThen executed if the index exists or after preparation is performed successfully
         */
        public void prepareIndexIfNeededThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
            try {
                // TODO we should improve this so we don't fire off a bunch of requests to do the same thing (create or update mappings)
                if (projectStatus == ProjectStatus.CLUSTER_NOT_RECOVERED) {
                    throw new ElasticsearchStatusException(
                        "Cluster state has not been recovered yet, cannot write to the [" + this.concreteIndexName + "] index",
                        RestStatus.SERVICE_UNAVAILABLE
                    );
                } else if (projectStatus == ProjectStatus.PROJECT_DOES_NOT_EXIST) {
                    throw new ElasticsearchStatusException(
                        "Project ["
                            + this.projectId
                            + "] is not available in cluster state, cannot write to the ["
                            + this.concreteIndexName
                            + "] index",
                        RestStatus.SERVICE_UNAVAILABLE
                    );
                } else if (this.indexExists() && this.isIndexUpToDate == false) {
                    throw new IllegalStateException(
                        "Index ["
                            + this.concreteIndexName
                            + "] is not on the current version."
                            + "Security features relying on the index will not be available until the upgrade API is run on the index"
                    );
                } else if (this.indexExists() == false) {
                    assert this.concreteIndexName != null;
                    final SystemIndexDescriptor descriptorForVersion = systemIndexDescriptor.getDescriptorCompatibleWith(
                        this.minClusterMappingVersion
                    );

                    if (descriptorForVersion == null) {
                        final String error = systemIndexDescriptor.getMinimumMappingsVersionMessage(
                            "create index",
                            this.minClusterMappingVersion
                        );
                        consumer.accept(new IllegalStateException(error));
                    } else {
                        logger.info(
                            "security index does not exist, creating [{}] with alias [{}]",
                            this.concreteIndexName,
                            descriptorForVersion.getAliasName()
                        );
                        // Although `TransportCreateIndexAction` is capable of automatically applying the right mappings, settings and
                        // aliases
                        // for system indices, we nonetheless specify them here so that the values from `descriptorForVersion` are used.
                        CreateIndexRequest request = new CreateIndexRequest(this.concreteIndexName).origin(descriptorForVersion.getOrigin())
                            .mapping(descriptorForVersion.getMappings())
                            .settings(descriptorForVersion.getSettings())
                            .alias(new Alias(descriptorForVersion.getAliasName()))
                            .waitForActiveShards(ActiveShardCount.ALL);

                        executeAsyncWithOrigin(
                            client.threadPool().getThreadContext(),
                            descriptorForVersion.getOrigin(),
                            request,
                            new ActionListener<CreateIndexResponse>() {
                                @Override
                                public void onResponse(CreateIndexResponse createIndexResponse) {
                                    if (createIndexResponse.isAcknowledged()) {
                                        andThen.run();
                                    } else {
                                        consumer.accept(new ElasticsearchException("Failed to create security index"));
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                                    if (cause instanceof ResourceAlreadyExistsException) {
                                        // the index already exists - it was probably just created so this
                                        // node hasn't yet received the cluster state update with the index
                                        andThen.run();
                                    } else {
                                        consumer.accept(e);
                                    }
                                }
                            },
                            client.admin().indices()::create
                        );
                    }
                } else if (this.mappingUpToDate == false) {
                    final SystemIndexDescriptor descriptorForVersion = systemIndexDescriptor.getDescriptorCompatibleWith(
                        this.minClusterMappingVersion
                    );

                    if (descriptorForVersion == null) {
                        final String error = systemIndexDescriptor.getMinimumMappingsVersionMessage(
                            "updating mapping",
                            this.minClusterMappingVersion
                        );
                        consumer.accept(new IllegalStateException(error));
                    } else {
                        logger.info(
                            "Index [{}] (alias [{}]) is not up to date. Updating mapping",
                            this.concreteIndexName,
                            descriptorForVersion.getAliasName()
                        );
                        PutMappingRequest request = new PutMappingRequest(this.concreteIndexName).source(
                            descriptorForVersion.getMappings(),
                            XContentType.JSON
                        ).origin(descriptorForVersion.getOrigin());
                        executeAsyncWithOrigin(
                            client.threadPool().getThreadContext(),
                            descriptorForVersion.getOrigin(),
                            request,
                            ActionListener.<AcknowledgedResponse>wrap(putMappingResponse -> {
                                if (putMappingResponse.isAcknowledged()) {
                                    andThen.run();
                                } else {
                                    consumer.accept(new IllegalStateException("put mapping request was not acknowledged"));
                                }
                            }, consumer),
                            client.admin().indices()::putMapping
                        );
                    }
                } else {
                    andThen.run();
                }
            } catch (Exception e) {
                consumer.accept(e);
            }
        }

        public RoleMappingsCleanupMigrationStatus getRoleMappingsCleanupMigrationStatus() {
            return this.roleMappingsCleanupMigrationStatus;
        }

        public boolean isEligibleSecurityMigration(SecurityMigrations.SecurityMigration securityMigration) {
            return this.securityFeatures.containsAll(securityMigration.nodeFeaturesRequired())
                && this.indexMappingVersion >= securityMigration.minMappingVersion()
                && securityMigration.checkPreConditions(this);
        }

        public boolean isReadyForSecurityMigration(SecurityMigrations.SecurityMigration securityMigration) {
            return this.indexAvailableForWrite
                && this.indexAvailableForSearch
                && this.isIndexUpToDate
                && this.indexExists()
                && isEligibleSecurityMigration(securityMigration);
        }

        /**
         * Waits up to {@code timeout} for the security index (in this project) to become available for search, based
         * on cluster state updates.
         * Notifies {@code listener} once the security index is available, or calls {@code onFailure} on {@code timeout}.
         */
        public void onIndexAvailableForSearch(ActionListener<Void> listener, TimeValue timeout) {
            logger.info(
                "Will wait for security index [{}] in project [{}] for [{}] to become available for search",
                getConcreteIndexName(),
                projectId,
                timeout
            );

            if (this.indexAvailableForSearch) {
                logger.debug("Security index [{}] in [{}] is already available", getConcreteIndexName(), projectId);
                listener.onResponse(null);
                return;
            }

            final AtomicBoolean isDone = new AtomicBoolean(false);
            final var indexAvailableForSearchListener = new StateConsumerWithCancellable() {
                @Override
                public void apply(ProjectId eventProjectId, IndexState previousState, IndexState nextState) {
                    if (eventProjectId.equals(IndexState.this.projectId) && nextState.indexAvailableForSearch) {
                        if (isDone.compareAndSet(false, true)) {
                            cancel();
                            removeStateListener(this);
                            listener.onResponse(null);
                        }
                    }
                }
            };
            // add listener _before_ registering timeout -- this way we are guaranteed it gets removed
            // (either by timeout below, or successful completion above)
            SecurityIndexManager.this.addStateListener(indexAvailableForSearchListener);

            // schedule failure handling on timeout -- keep reference to cancellable so a successful completion can cancel the timeout
            indexAvailableForSearchListener.setCancellable(client.threadPool().schedule(() -> {
                if (isDone.compareAndSet(false, true)) {
                    removeStateListener(indexAvailableForSearchListener);
                    listener.onFailure(
                        new ElasticsearchTimeoutException(
                            "timed out waiting for security index [" + this.getConcreteIndexName() + "] to become available for search"
                        )
                    );
                }
            }, timeout, client.threadPool().generic()));
        }
    }

    /**
     * Retrieves the project scoped index state for the {@link ProjectResolver#getProjectId() current project}.
     * @see #getProject(ProjectId)
     */
    public IndexState forCurrentProject() {
        return getProject(projectResolver.getProjectId());
    }

    /**
     * Retrieves the project scoped index state.
     * This is a point-in-time copy of the current state of the index in that project.
     * Its internal representation will not change even if the index's underlying state changes in the cluster metadata.
     */
    public IndexState getProject(ProjectId project) {
        return getProjectState(project, stateByProject);
    }

    private IndexState getProjectState(ProjectId project, Map<ProjectId, IndexState> byProject) {
        if (byProject == null) {
            return unavailableState(project, ProjectStatus.CLUSTER_NOT_RECOVERED);
        } else {
            return Objects.requireNonNullElseGet(
                byProject.get(project),
                () -> unavailableState(project, ProjectStatus.PROJECT_DOES_NOT_EXIST)
            );
        }
    }

    public String aliasName() {
        return systemIndexDescriptor.getAliasName();
    }

    /**
     * Add a listener for notifications on state changes to the configured index.
     *
     * The previous and current state are provided.
     */
    public void addStateListener(TriConsumer<ProjectId, IndexState, IndexState> listener) {
        stateChangeListeners.add(listener);
    }

    /**
     * Remove a listener from notifications on state changes to the configured index.
     *
     */
    public void removeStateListener(TriConsumer<ProjectId, IndexState, IndexState> listener) {
        stateChangeListeners.remove(listener);
    }

    /**
     * Get the minimum security index mapping version in the cluster
     */
    @FixForMultiProject
    private SystemIndexDescriptor.MappingsVersion getMinSecurityIndexMappingVersion(ProjectState projectState) {
        SystemIndexDescriptor.MappingsVersion mappingsVersion = projectState.cluster()
            .getMinSystemIndexMappingVersions()
            .get(systemIndexDescriptor.getPrimaryIndex());
        return mappingsVersion == null ? new SystemIndexDescriptor.MappingsVersion(1, 0) : mappingsVersion;
    }

    /**
     * Check if the index was created on the latest index version available in the cluster
     */

    private static boolean isCreatedOnLatestVersion(IndexMetadata indexMetadata) {
        final IndexVersion indexVersionCreated = indexMetadata != null
            ? SETTING_INDEX_VERSION_CREATED.get(indexMetadata.getSettings())
            : null;
        return indexVersionCreated != null && indexVersionCreated.onOrAfter(IndexVersion.current());
    }

    /**
     * Check if a role mappings cleanup migration is needed or has already been performed and if the project is ready for a cleanup
     * migration
     *
     * @param projectState current project state
     * @param migrationsVersion current migration version
     */
    static RoleMappingsCleanupMigrationStatus getRoleMappingsCleanupMigrationStatus(ProjectState projectState, int migrationsVersion) {
        // Migration already finished
        if (migrationsVersion >= SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION) {
            return RoleMappingsCleanupMigrationStatus.DONE;
        }

        @FixForMultiProject
        // Need to update this when we have per-project "reservedStateMetadata"
        ReservedStateMetadata fileSettingsMetadata = projectState.cluster()
            .metadata()
            .reservedStateMetadata()
            .get(FILE_SETTINGS_METADATA_NAMESPACE);
        boolean hasFileSettingsMetadata = fileSettingsMetadata != null;
        // If there is no fileSettingsMetadata, there should be no reserved state (this is to catch bugs related to
        // name changes to FILE_SETTINGS_METADATA_NAMESPACE)
        assert hasFileSettingsMetadata || projectState.cluster().metadata().reservedStateMetadata().isEmpty()
            : "ReservedStateMetadata contains unknown namespace";

        // If no file based role mappings available -> migration not needed
        if (hasFileSettingsMetadata == false || fileSettingsMetadata.keys(ReservedRoleMappingAction.NAME).isEmpty()) {
            return RoleMappingsCleanupMigrationStatus.SKIP;
        }

        RoleMappingMetadata roleMappingMetadata = RoleMappingMetadata.getFromProject(projectState.metadata());

        // If there are file based role mappings, make sure they have the latest format (name available) and that they have all been
        // synced to cluster state (same size as the reserved state keys)
        if (roleMappingMetadata.getRoleMappings().size() == fileSettingsMetadata.keys(ReservedRoleMappingAction.NAME).size()
            && roleMappingMetadata.hasAnyMappingWithFallbackName() == false) {
            return RoleMappingsCleanupMigrationStatus.READY;
        }

        // If none of the above conditions are met, wait for a state change to re-evaluate if the cluster is ready for migration
        return RoleMappingsCleanupMigrationStatus.NOT_READY;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState clusterState = event.state();
        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think we don't have the
            // .security index but they may not have been restored from the cluster state on disk
            logger.debug("security index manager waiting until state has been recovered");
            return;
        }

        final Map<ProjectId, IndexState> previousStateByProject = stateByProject;
        final List<Runnable> notifications = new ArrayList<>();

        if (previousStateByProject == null) {
            stateRecoveredListeners.forEach(listener -> notifications.add(() -> {
                listener.accept(this);
                stateRecoveredListeners.remove(listener);
            }));
        }

        final Map<ProjectId, IndexState> newStateByProject = Maps.newMapWithExpectedSize(clusterState.metadata().projects().size());
        for (ProjectId projectId : clusterState.metadata().projects().keySet()) {
            final IndexState previousState = getProjectState(projectId, previousStateByProject);
            final IndexState newState = updateProjectState(clusterState.projectState(projectId));

            if (newState.equals(previousState) == false) {
                notifications.add(() -> {
                    for (var listener : stateChangeListeners) {
                        listener.apply(projectId, previousState, newState);
                    }
                });
            }
            newStateByProject.put(projectId, newState);
        }
        if (previousStateByProject != null) {
            for (ProjectId projectId : previousStateByProject.keySet()) {
                if (newStateByProject.containsKey(projectId) == false) {
                    notifications.add(() -> {
                        for (var listener : stateChangeListeners) {
                            listener.apply(
                                projectId,
                                previousStateByProject.get(projectId),
                                unavailableState(projectId, ProjectStatus.PROJECT_DOES_NOT_EXIST)
                            );
                        }
                    });
                }
            }
        }

        this.stateByProject = Collections.unmodifiableMap(newStateByProject);

        // Need to run the notifications after updating stateByProject so that any calls to "getProject" will return the
        // correct (updated) value
        notifications.forEach(Runnable::run);
    }

    private IndexState updateProjectState(ProjectState project) {
        final IndexMetadata indexMetadata = resolveConcreteIndex(systemIndexDescriptor.getAliasName(), project.metadata());
        final boolean createdOnLatestVersion = isCreatedOnLatestVersion(indexMetadata);
        final Instant creationTime = indexMetadata != null ? Instant.ofEpochMilli(indexMetadata.getCreationDate()) : null;
        final boolean isIndexUpToDate = indexMetadata == null
            || INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == systemIndexDescriptor.getIndexFormat();
        Tuple<Boolean, Boolean> available = checkIndexAvailable(project);
        final boolean indexAvailableForWrite = available.v1();
        final boolean indexAvailableForSearch = available.v2();
        final int migrationsVersion = getMigrationVersionFromIndexMetadata(indexMetadata);
        final RoleMappingsCleanupMigrationStatus roleMappingsCleanupMigrationStatus = getRoleMappingsCleanupMigrationStatus(
            project,
            migrationsVersion
        );
        final boolean mappingIsUpToDate = indexMetadata == null || checkIndexMappingUpToDate(project);
        final SystemIndexDescriptor.MappingsVersion minClusterMappingVersion = getMinSecurityIndexMappingVersion(project);
        final int indexMappingVersion = loadIndexMappingVersion(systemIndexDescriptor.getAliasName(), project.metadata());
        final String concreteIndexName = indexMetadata == null
            ? systemIndexDescriptor.getPrimaryIndex()
            : indexMetadata.getIndex().getName();
        final ClusterHealthStatus indexHealth;
        final IndexMetadata.State indexState;
        if (indexMetadata == null) {
            // Index does not exist
            indexState = null;
            indexHealth = null;
        } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            indexState = IndexMetadata.State.CLOSE;
            indexHealth = null;
            logger.warn("Index [{}] is closed. This is likely to prevent security from functioning correctly", concreteIndexName);
        } else {
            indexState = IndexMetadata.State.OPEN;
            final IndexRoutingTable routingTable = project.routingTable().index(indexMetadata.getIndex());
            indexHealth = new ClusterIndexHealth(indexMetadata, routingTable).getStatus();
        }
        final String indexUUID = indexMetadata != null ? indexMetadata.getIndexUUID() : null;
        final IndexState newState = new IndexState(
            project.projectId(),
            ProjectStatus.PROJECT_AVAILABLE,
            creationTime,
            isIndexUpToDate,
            indexAvailableForSearch,
            indexAvailableForWrite,
            mappingIsUpToDate,
            createdOnLatestVersion,
            roleMappingsCleanupMigrationStatus,
            migrationsVersion,
            minClusterMappingVersion,
            indexMappingVersion,
            concreteIndexName,
            indexHealth,
            indexState,
            indexUUID,
            allSecurityFeatures.stream()
                .filter(feature -> featureService.clusterHasFeature(project.cluster(), feature))
                .collect(Collectors.toSet())
        );
        return newState;
    }

    public static int getMigrationVersionFromIndexMetadata(IndexMetadata indexMetadata) {
        Map<String, String> customMetadata = indexMetadata == null ? null : indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY);
        if (customMetadata == null) {
            return 0;
        }
        String migrationVersion = customMetadata.get(MIGRATION_VERSION_CUSTOM_DATA_KEY);
        return migrationVersion == null ? 0 : Integer.parseInt(migrationVersion);
    }

    public void whenProjectStateAvailable(ProjectId targetProject, Consumer<IndexState> stateConsumer) {
        TriConsumer<ProjectId, IndexState, IndexState> stateChangeListener = new TriConsumer<>() {
            @Override
            public void apply(ProjectId projectId, IndexState previousState, IndexState nextState) {
                if (projectId.equals(targetProject) == false) {
                    return;
                }
                if (nextState.isProjectAvailable()) {
                    stateChangeListeners.remove(this);
                    stateConsumer.accept(nextState);
                }
            }
        };
        stateChangeListeners.add(stateChangeListener);
    }

    // pkg-private for testing
    List<TriConsumer<ProjectId, IndexState, IndexState>> getStateChangeListeners() {
        return stateChangeListeners;
    }

    /**
     * This class ensures that if cancel() is called _before_ setCancellable(), the passed-in cancellable is still correctly cancelled on
     * a subsequent setCancellable() call.
     */
    // pkg-private for testing
    abstract static class StateConsumerWithCancellable implements TriConsumer<ProjectId, IndexState, IndexState>, Scheduler.Cancellable {
        private volatile Scheduler.ScheduledCancellable cancellable;
        private volatile boolean cancelled = false;

        void setCancellable(Scheduler.ScheduledCancellable cancellable) {
            this.cancellable = cancellable;
            if (cancelled) {
                cancel();
            }
        }

        public boolean cancel() {
            cancelled = true;
            if (cancellable != null) {
                // cancellable is idempotent, so it's fine to potentially call it multiple times
                return cancellable.cancel();
            }
            return isCancelled();
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    private Tuple<Boolean, Boolean> checkIndexAvailable(ProjectState project) {
        final String aliasName = systemIndexDescriptor.getAliasName();
        IndexMetadata metadata = resolveConcreteIndex(aliasName, project.metadata());
        if (metadata == null) {
            logger.debug("Index [{}] is not available in project [{}] - no metadata", aliasName, project.projectId());
            return new Tuple<>(false, false);
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn("Index [{}] is closed in project [{}]", aliasName, project.projectId());
            return new Tuple<>(false, false);
        }
        boolean allPrimaryShards = false;
        boolean searchShards = false;
        final IndexRoutingTable routingTable = project.routingTable().index(metadata.getIndex());
        if (routingTable != null && routingTable.allPrimaryShardsActive()) {
            allPrimaryShards = true;
        }
        if (routingTable != null && routingTable.readyForSearch()) {
            searchShards = true;
        }
        if (allPrimaryShards == false || searchShards == false) {
            logger.debug(
                "Index [{}] is not fully available in project [{}]. all primary shards available [{}], search shards available, [{}]",
                aliasName,
                project.projectId(),
                allPrimaryShards,
                searchShards
            );
        }
        return new Tuple<>(allPrimaryShards, searchShards);
    }

    /**
     * Detect if the mapping in the security index is outdated. If it's outdated it means that whatever is in cluster state is more recent.
     * There could be several nodes on different ES versions (mixed cluster) supporting different mapping versions, so only return false if
     * min version in the cluster is more recent than what's in the security index.
     */
    private boolean checkIndexMappingUpToDate(ProjectState projectState) {
        // Get descriptor compatible with the min version in the cluster
        final SystemIndexDescriptor descriptor = systemIndexDescriptor.getDescriptorCompatibleWith(
            getMinSecurityIndexMappingVersion(projectState)
        );
        if (descriptor == null) {
            return false;
        }
        return descriptor.getMappingsVersion().version() <= loadIndexMappingVersion(
            systemIndexDescriptor.getAliasName(),
            projectState.metadata()
        );
    }

    private static int loadIndexMappingVersion(String aliasName, ProjectMetadata projectMetadata) {
        IndexMetadata indexMetadata = resolveConcreteIndex(aliasName, projectMetadata);
        if (indexMetadata != null) {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                return readMappingVersion(aliasName, mappingMetadata);
            }
        }
        return 0;
    }

    private static int readMappingVersion(String indexName, MappingMetadata mappingMetadata) {
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
        if (meta == null) {
            logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
            throw new IllegalStateException("Cannot read managed_index_mappings_version string in index " + indexName);
        }
        // If null, no value has been set in the index yet, so return 0 to trigger put mapping
        final Integer value = (Integer) meta.get(VERSION_META_KEY);
        return value == null ? 0 : value;
    }

    /**
     * Resolves a concrete index name or alias to a {@link IndexMetadata} instance.  Requires
     * that if supplied with an alias, the alias resolves to at most one concrete index.
     */
    public static IndexMetadata resolveConcreteIndex(final String indexOrAliasName, final ProjectMetadata project) {
        final IndexAbstraction indexAbstraction = project.getIndicesLookup().get(indexOrAliasName);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + indexOrAliasName + "] points to more than one index: " + indices);
            }
            return project.index(indices.get(0));
        }
        return null;
    }

    /**
     * Return true if the state moves from an unhealthy ("RED") index state to a healthy ("non-RED") state.
     */
    public static boolean isMoveFromRedToNonRed(IndexState previousState, IndexState currentState) {
        return (previousState.indexHealth == null || previousState.indexHealth == ClusterHealthStatus.RED)
            && currentState.indexHealth != null
            && currentState.indexHealth != ClusterHealthStatus.RED;
    }

    /**
     * Return true if the state moves from the index existing to the index not existing.
     */
    public static boolean isIndexDeleted(IndexState previousState, IndexState currentState) {
        return previousState.indexHealth != null && currentState.indexHealth == null;
    }
}
