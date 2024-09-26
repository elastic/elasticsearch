/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.xpack.security.SecurityFeatures;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.State.UNRECOVERED_STATE;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MIGRATION_FRAMEWORK;

/**
 * Manages the lifecycle, mapping and data upgrades/migrations of the {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS}
 * and {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS} alias-index pair.
 */
public class SecurityIndexManager implements ClusterStateListener {

    public static final String SECURITY_VERSION_STRING = "security-version";

    private static final Logger logger = LogManager.getLogger(SecurityIndexManager.class);

    /**
     * When checking availability, check for availability of search or availability of all primaries
     **/
    public enum Availability {
        SEARCH_SHARDS,
        PRIMARY_SHARDS
    }

    private final Client client;
    private final SystemIndexDescriptor systemIndexDescriptor;

    private final List<BiConsumer<State, State>> stateChangeListeners = new CopyOnWriteArrayList<>();

    private volatile State state;
    private final boolean defensiveCopy;
    private final FeatureService featureService;

    private final Set<NodeFeature> allSecurityFeatures = new SecurityFeatures().getFeatures();

    public static SecurityIndexManager buildSecurityIndexManager(
        Client client,
        ClusterService clusterService,
        FeatureService featureService,
        SystemIndexDescriptor descriptor
    ) {
        final SecurityIndexManager securityIndexManager = new SecurityIndexManager(
            featureService,
            client,
            descriptor,
            State.UNRECOVERED_STATE,
            false
        );
        clusterService.addListener(securityIndexManager);
        return securityIndexManager;
    }

    private SecurityIndexManager(
        FeatureService featureService,
        Client client,
        SystemIndexDescriptor descriptor,
        State state,
        boolean defensiveCopy
    ) {
        this.featureService = featureService;
        this.client = client;
        this.state = state;
        this.systemIndexDescriptor = descriptor;
        this.defensiveCopy = defensiveCopy;
    }

    /**
     * Creates a defensive to protect against the underlying state changes. Should be called prior to making decisions and that same copy
     * should be reused for multiple checks in the same workflow.
     */
    public SecurityIndexManager defensiveCopy() {
        return new SecurityIndexManager(null, null, systemIndexDescriptor, state, true);
    }

    public String aliasName() {
        return systemIndexDescriptor.getAliasName();
    }

    public boolean indexExists() {
        return this.state.indexExists();
    }

    public boolean indexIsClosed() {
        return this.state.indexState == IndexMetadata.State.CLOSE;
    }

    public Instant getCreationTime() {
        return this.state.creationTime;
    }

    /**
     * Returns whether the index is on the current format if it exists. If the index does not exist
     * we treat the index as up to date as we expect it to be created with the current format.
     */
    public boolean isIndexUpToDate() {
        return this.state.isIndexUpToDate;
    }

    /**
     * Optimization to avoid making unnecessary calls when we know the underlying shard state. This call will check that the index exists,
     * is discoverable from the alias, is not closed, and will determine if available based on the {@link Availability} parameter.
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
                return this.state.indexAvailableForSearch;
            }
            case PRIMARY_SHARDS -> {
                return this.state.indexAvailableForWrite;
            }
        }
        // can never happen
        throw new IllegalStateException("Unexpected availability enumeration. This is bug, please contact support.");
    }

    public boolean isMappingUpToDate() {
        return this.state.mappingUpToDate;
    }

    public boolean isStateRecovered() {
        return this.state != State.UNRECOVERED_STATE;
    }

    public boolean isMigrationsVersionAtLeast(Integer expectedMigrationsVersion) {
        return indexExists() && this.state.migrationsVersion.compareTo(expectedMigrationsVersion) >= 0;
    }

    public boolean isCreatedOnLatestVersion() {
        return this.state.createdOnLatestVersion;
    }

    public ElasticsearchException getUnavailableReason(Availability availability) {
        // ensure usage of a local copy so all checks execute against the same state!
        if (defensiveCopy == false) {
            throw new IllegalStateException("caller must make sure to use a defensive copy");
        }
        final State state = this.state;
        if (state.indexState == IndexMetadata.State.CLOSE) {
            return new IndexClosedException(new Index(state.concreteIndexName, ClusterState.UNKNOWN_UUID));
        } else if (state.indexExists()) {
            assert state.indexAvailableForSearch == false || state.indexAvailableForWrite == false;
            if (Availability.PRIMARY_SHARDS.equals(availability) && state.indexAvailableForWrite == false) {
                return new UnavailableShardsException(
                    null,
                    "at least one primary shard for the index [" + state.concreteIndexName + "] is unavailable"
                );
            } else if (Availability.SEARCH_SHARDS.equals(availability) && state.indexAvailableForSearch == false) {
                // The current behavior is that when primaries are unavailable and replicas can not be promoted then
                // any replicas will be marked as unavailable as well. This is applicable in stateless where there index only primaries
                // with non-promotable replicas (i.e. search only shards). In the case "at least one search ... is unavailable" is
                // a technically correct statement, but it may be unavailable because it is not promotable and the primary is unavailable
                return new UnavailableShardsException(
                    null,
                    "at least one search shard for the index [" + state.concreteIndexName + "] is unavailable"
                );
            } else {
                // should never happen
                throw new IllegalStateException("caller must ensure original availability matches the current availability");
            }
        } else {
            return new IndexNotFoundException(state.concreteIndexName);
        }
    }

    /**
     * Add a listener for notifications on state changes to the configured index.
     *
     * The previous and current state are provided.
     */
    public void addStateListener(BiConsumer<State, State> listener) {
        stateChangeListeners.add(listener);
    }

    /**
     * Remove a listener from notifications on state changes to the configured index.
     *
     */
    public void removeStateListener(BiConsumer<State, State> listener) {
        stateChangeListeners.remove(listener);
    }

    /**
     * Get the minimum security index mapping version in the cluster
     */
    private SystemIndexDescriptor.MappingsVersion getMinSecurityIndexMappingVersion(ClusterState clusterState) {
        SystemIndexDescriptor.MappingsVersion mappingsVersion = clusterState.getMinSystemIndexMappingVersions()
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think we don't have the
            // .security index but they may not have been restored from the cluster state on disk
            logger.debug("security index manager waiting until state has been recovered");
            return;
        }
        final State previousState = state;
        final IndexMetadata indexMetadata = resolveConcreteIndex(systemIndexDescriptor.getAliasName(), event.state().metadata());
        final boolean createdOnLatestVersion = isCreatedOnLatestVersion(indexMetadata);
        final Instant creationTime = indexMetadata != null ? Instant.ofEpochMilli(indexMetadata.getCreationDate()) : null;
        final boolean isIndexUpToDate = indexMetadata == null
            || INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == systemIndexDescriptor.getIndexFormat();
        Tuple<Boolean, Boolean> available = checkIndexAvailable(event.state());
        final boolean indexAvailableForWrite = available.v1();
        final boolean indexAvailableForSearch = available.v2();
        final boolean mappingIsUpToDate = indexMetadata == null || checkIndexMappingUpToDate(event.state());
        final int migrationsVersion = getMigrationVersionFromIndexMetadata(indexMetadata);
        final SystemIndexDescriptor.MappingsVersion minClusterMappingVersion = getMinSecurityIndexMappingVersion(event.state());
        final int indexMappingVersion = loadIndexMappingVersion(systemIndexDescriptor.getAliasName(), event.state());
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
            final IndexRoutingTable routingTable = event.state().getRoutingTable().index(indexMetadata.getIndex());
            indexHealth = new ClusterIndexHealth(indexMetadata, routingTable).getStatus();
        }
        final String indexUUID = indexMetadata != null ? indexMetadata.getIndexUUID() : null;
        final State newState = new State(
            creationTime,
            isIndexUpToDate,
            indexAvailableForSearch,
            indexAvailableForWrite,
            mappingIsUpToDate,
            createdOnLatestVersion,
            migrationsVersion,
            minClusterMappingVersion,
            indexMappingVersion,
            concreteIndexName,
            indexHealth,
            indexState,
            indexUUID,
            allSecurityFeatures.stream()
                .filter(feature -> featureService.clusterHasFeature(event.state(), feature))
                .collect(Collectors.toSet())
        );
        this.state = newState;

        if (newState.equals(previousState) == false) {
            for (BiConsumer<State, State> listener : stateChangeListeners) {
                listener.accept(previousState, newState);
            }
        }
    }

    public static int getMigrationVersionFromIndexMetadata(IndexMetadata indexMetadata) {
        Map<String, String> customMetadata = indexMetadata == null ? null : indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY);
        if (customMetadata == null) {
            return 0;
        }
        String migrationVersion = customMetadata.get(MIGRATION_VERSION_CUSTOM_DATA_KEY);
        return migrationVersion == null ? 0 : Integer.parseInt(migrationVersion);
    }

    public void onStateRecovered(Consumer<State> recoveredStateConsumer) {
        BiConsumer<State, State> stateChangeListener = new BiConsumer<>() {
            @Override
            public void accept(State previousState, State nextState) {
                boolean stateJustRecovered = previousState == UNRECOVERED_STATE && nextState != UNRECOVERED_STATE;
                boolean stateAlreadyRecovered = previousState != UNRECOVERED_STATE;
                if (stateJustRecovered) {
                    recoveredStateConsumer.accept(nextState);
                } else if (stateAlreadyRecovered) {
                    stateChangeListeners.remove(this);
                }
            }
        };
        stateChangeListeners.add(stateChangeListener);
    }

    /**
     * Waits up to {@code timeout} for the security index to become available for search, based on cluster state updates.
     * Notifies {@code listener} once the security index is available, or calls {@code onFailure} on {@code timeout}.
     */
    public void onIndexAvailableForSearch(ActionListener<Void> listener, TimeValue timeout) {
        logger.info("Will wait for security index [{}] for [{}] to become available for search", getConcreteIndexName(), timeout);

        if (state.indexAvailableForSearch) {
            logger.debug("Security index [{}] is already available", getConcreteIndexName());
            listener.onResponse(null);
            return;
        }

        final AtomicBoolean isDone = new AtomicBoolean(false);
        final var indexAvailableForSearchListener = new StateConsumerWithCancellable() {
            @Override
            public void accept(SecurityIndexManager.State previousState, SecurityIndexManager.State nextState) {
                if (nextState.indexAvailableForSearch) {
                    if (isDone.compareAndSet(false, true)) {
                        cancel();
                        removeStateListener(this);
                        listener.onResponse(null);
                    }
                }
            }
        };
        // add listener _before_ registering timeout -- this way we are guaranteed it gets removed (either by timeout below, or successful
        // completion above)
        addStateListener(indexAvailableForSearchListener);

        // schedule failure handling on timeout -- keep reference to cancellable so a successful completion can cancel the timeout
        indexAvailableForSearchListener.setCancellable(client.threadPool().schedule(() -> {
            if (isDone.compareAndSet(false, true)) {
                removeStateListener(indexAvailableForSearchListener);
                listener.onFailure(
                    new ElasticsearchTimeoutException(
                        "timed out waiting for security index [" + getConcreteIndexName() + "] to become available for search"
                    )
                );
            }
        }, timeout, client.threadPool().generic()));
    }

    // pkg-private for testing
    List<BiConsumer<State, State>> getStateChangeListeners() {
        return stateChangeListeners;
    }

    /**
     * This class ensures that if cancel() is called _before_ setCancellable(), the passed-in cancellable is still correctly cancelled on
     * a subsequent setCancellable() call.
     */
    // pkg-private for testing
    abstract static class StateConsumerWithCancellable
        implements
            BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State>,
            Scheduler.Cancellable {
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

    private Tuple<Boolean, Boolean> checkIndexAvailable(ClusterState state) {
        final String aliasName = systemIndexDescriptor.getAliasName();
        IndexMetadata metadata = resolveConcreteIndex(aliasName, state.metadata());
        if (metadata == null) {
            logger.debug("Index [{}] is not available - no metadata", aliasName);
            return new Tuple<>(false, false);
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn("Index [{}] is closed", aliasName);
            return new Tuple<>(false, false);
        }
        boolean allPrimaryShards = false;
        boolean searchShards = false;
        final IndexRoutingTable routingTable = state.routingTable().index(metadata.getIndex());
        if (routingTable != null && routingTable.allPrimaryShardsActive()) {
            allPrimaryShards = true;
        }
        if (routingTable != null && routingTable.readyForSearch(state)) {
            searchShards = true;
        }
        if (allPrimaryShards == false || searchShards == false) {
            logger.debug(
                "Index [{}] is not fully available. all primary shards available [{}], search shards available, [{}]",
                aliasName,
                allPrimaryShards,
                searchShards
            );
        }
        return new Tuple<>(allPrimaryShards, searchShards);
    }

    public boolean isEligibleSecurityMigration(SecurityMigrations.SecurityMigration securityMigration) {
        return state.securityFeatures.containsAll(securityMigration.nodeFeaturesRequired())
            && state.indexMappingVersion >= securityMigration.minMappingVersion();
    }

    public boolean isReadyForSecurityMigration(SecurityMigrations.SecurityMigration securityMigration) {
        return state.indexAvailableForWrite
            && state.indexAvailableForSearch
            && state.isIndexUpToDate
            && state.indexExists()
            && state.securityFeatures.contains(SECURITY_MIGRATION_FRAMEWORK)
            && isEligibleSecurityMigration(securityMigration);
    }

    /**
     * Detect if the mapping in the security index is outdated. If it's outdated it means that whatever is in cluster state is more recent.
     * There could be several nodes on different ES versions (mixed cluster) supporting different mapping versions, so only return false if
     * min version in the cluster is more recent than what's in the security index.
     */
    private boolean checkIndexMappingUpToDate(ClusterState clusterState) {
        // Get descriptor compatible with the min version in the cluster
        final SystemIndexDescriptor descriptor = systemIndexDescriptor.getDescriptorCompatibleWith(
            getMinSecurityIndexMappingVersion(clusterState)
        );
        if (descriptor == null) {
            return false;
        }
        return descriptor.getMappingsVersion().version() <= loadIndexMappingVersion(systemIndexDescriptor.getAliasName(), clusterState);
    }

    private static int loadIndexMappingVersion(String aliasName, ClusterState clusterState) {
        IndexMetadata indexMetadata = resolveConcreteIndex(aliasName, clusterState.metadata());
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
    private static IndexMetadata resolveConcreteIndex(final String indexOrAliasName, final Metadata metadata) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexOrAliasName);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + indexOrAliasName + "] points to more than one index: " + indices);
            }
            return metadata.index(indices.get(0));
        }
        return null;
    }

    /**
     * Validates that the index is up to date and does not need to be migrated. If it is not, the
     * consumer is called with an exception. If the index is up to date, the runnable will
     * be executed. <b>NOTE:</b> this method does not check the availability of the index; this check
     * is left to the caller so that this condition can be handled appropriately.
     */
    public void checkIndexVersionThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
        final State state = this.state; // use a local copy so all checks execute against the same state!
        if (state.indexExists() && state.isIndexUpToDate == false) {
            consumer.accept(
                new IllegalStateException(
                    "Index ["
                        + state.concreteIndexName
                        + "] is not on the current version. Security features relying on the index"
                        + " will not be available until the upgrade API is run on the index"
                )
            );
        } else {
            andThen.run();
        }
    }

    public String getConcreteIndexName() {
        return state.concreteIndexName;
    }

    /**
     * Prepares the index by creating it if it doesn't exist, then executes the runnable.
     * @param consumer a handler for any exceptions that are raised either during preparation or execution
     * @param andThen executed if the index exists or after preparation is performed successfully
     */
    public void prepareIndexIfNeededThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
        final State state = this.state; // use a local copy so all checks execute against the same state!
        try {
            // TODO we should improve this so we don't fire off a bunch of requests to do the same thing (create or update mappings)
            if (state == State.UNRECOVERED_STATE) {
                throw new ElasticsearchStatusException(
                    "Cluster state has not been recovered yet, cannot write to the [" + state.concreteIndexName + "] index",
                    RestStatus.SERVICE_UNAVAILABLE
                );
            } else if (state.indexExists() && state.isIndexUpToDate == false) {
                throw new IllegalStateException(
                    "Index ["
                        + state.concreteIndexName
                        + "] is not on the current version."
                        + "Security features relying on the index will not be available until the upgrade API is run on the index"
                );
            } else if (state.indexExists() == false) {
                assert state.concreteIndexName != null;
                final SystemIndexDescriptor descriptorForVersion = systemIndexDescriptor.getDescriptorCompatibleWith(
                    state.minClusterMappingVersion
                );

                if (descriptorForVersion == null) {
                    final String error = systemIndexDescriptor.getMinimumMappingsVersionMessage("create index");
                    consumer.accept(new IllegalStateException(error));
                } else {
                    logger.info(
                        "security index does not exist, creating [{}] with alias [{}]",
                        state.concreteIndexName,
                        descriptorForVersion.getAliasName()
                    );
                    // Although `TransportCreateIndexAction` is capable of automatically applying the right mappings, settings and aliases
                    // for system indices, we nonetheless specify them here so that the values from `descriptorForVersion` are used.
                    CreateIndexRequest request = new CreateIndexRequest(state.concreteIndexName).origin(descriptorForVersion.getOrigin())
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
            } else if (state.mappingUpToDate == false) {
                final SystemIndexDescriptor descriptorForVersion = systemIndexDescriptor.getDescriptorCompatibleWith(
                    state.minClusterMappingVersion
                );

                if (descriptorForVersion == null) {
                    final String error = systemIndexDescriptor.getMinimumMappingsVersionMessage("updating mapping");
                    consumer.accept(new IllegalStateException(error));
                } else {
                    logger.info(
                        "Index [{}] (alias [{}]) is not up to date. Updating mapping",
                        state.concreteIndexName,
                        descriptorForVersion.getAliasName()
                    );
                    PutMappingRequest request = new PutMappingRequest(state.concreteIndexName).source(
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

    /**
     * Return true if the state moves from an unhealthy ("RED") index state to a healthy ("non-RED") state.
     */
    public static boolean isMoveFromRedToNonRed(State previousState, State currentState) {
        return (previousState.indexHealth == null || previousState.indexHealth == ClusterHealthStatus.RED)
            && currentState.indexHealth != null
            && currentState.indexHealth != ClusterHealthStatus.RED;
    }

    /**
     * Return true if the state moves from the index existing to the index not existing.
     */
    public static boolean isIndexDeleted(State previousState, State currentState) {
        return previousState.indexHealth != null && currentState.indexHealth == null;
    }

    /**
     * State of the security index.
     */
    public static class State {
        public static final State UNRECOVERED_STATE = new State(
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
            Set.of()
        );
        public final Instant creationTime;
        public final boolean isIndexUpToDate;
        public final boolean indexAvailableForSearch;
        public final boolean indexAvailableForWrite;
        public final boolean mappingUpToDate;
        public final boolean createdOnLatestVersion;
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

        public State(
            Instant creationTime,
            boolean isIndexUpToDate,
            boolean indexAvailableForSearch,
            boolean indexAvailableForWrite,
            boolean mappingUpToDate,
            boolean createdOnLatestVersion,
            Integer migrationsVersion,
            SystemIndexDescriptor.MappingsVersion minClusterMappingVersion,
            Integer indexMappingVersion,
            String concreteIndexName,
            ClusterHealthStatus indexHealth,
            IndexMetadata.State indexState,
            String indexUUID,
            Set<NodeFeature> securityFeatures
        ) {
            this.creationTime = creationTime;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailableForSearch = indexAvailableForSearch;
            this.indexAvailableForWrite = indexAvailableForWrite;
            this.mappingUpToDate = mappingUpToDate;
            this.migrationsVersion = migrationsVersion;
            this.createdOnLatestVersion = createdOnLatestVersion;
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return Objects.equals(creationTime, state.creationTime)
                && isIndexUpToDate == state.isIndexUpToDate
                && indexAvailableForSearch == state.indexAvailableForSearch
                && indexAvailableForWrite == state.indexAvailableForWrite
                && mappingUpToDate == state.mappingUpToDate
                && createdOnLatestVersion == state.createdOnLatestVersion
                && Objects.equals(indexMappingVersion, state.indexMappingVersion)
                && Objects.equals(migrationsVersion, state.migrationsVersion)
                && Objects.equals(minClusterMappingVersion, state.minClusterMappingVersion)
                && Objects.equals(concreteIndexName, state.concreteIndexName)
                && indexHealth == state.indexHealth
                && indexState == state.indexState
                && Objects.equals(securityFeatures, state.securityFeatures);
        }

        public boolean indexExists() {
            return creationTime != null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                creationTime,
                isIndexUpToDate,
                indexAvailableForSearch,
                indexAvailableForWrite,
                mappingUpToDate,
                createdOnLatestVersion,
                migrationsVersion,
                minClusterMappingVersion,
                indexMappingVersion,
                concreteIndexName,
                indexHealth,
                securityFeatures
            );
        }

        @Override
        public String toString() {
            return "State{"
                + "creationTime="
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
                + ", migrationsVersion="
                + migrationsVersion
                + ", minClusterMappingVersion="
                + minClusterMappingVersion
                + ", indexMappingVersion="
                + indexMappingVersion
                + ", concreteIndexName='"
                + concreteIndexName
                + '\''
                + ", indexHealth="
                + indexHealth
                + ", indexState="
                + indexState
                + ", indexUUID='"
                + indexUUID
                + '\''
                + ", securityFeatures="
                + securityFeatures
                + '}';
        }
    }
}
