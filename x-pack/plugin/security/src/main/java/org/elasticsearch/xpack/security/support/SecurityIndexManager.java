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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.State.UNRECOVERED_STATE;

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

    public static SecurityIndexManager buildSecurityIndexManager(
        Client client,
        ClusterService clusterService,
        SystemIndexDescriptor descriptor
    ) {
        final SecurityIndexManager securityIndexManager = new SecurityIndexManager(client, descriptor, State.UNRECOVERED_STATE, false);
        clusterService.addListener(securityIndexManager);
        return securityIndexManager;
    }

    private SecurityIndexManager(Client client, SystemIndexDescriptor descriptor, State state, boolean defensiveCopy) {
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
        return new SecurityIndexManager(null, systemIndexDescriptor, state, true);
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
        final Instant creationTime = indexMetadata != null ? Instant.ofEpochMilli(indexMetadata.getCreationDate()) : null;
        final boolean isIndexUpToDate = indexMetadata == null
            || INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == systemIndexDescriptor.getIndexFormat();
        Tuple<Boolean, Boolean> available = checkIndexAvailable(event.state());
        final boolean indexAvailableForWrite = available.v1();
        final boolean indexAvailableForSearch = available.v2();
        final boolean mappingIsUpToDate = indexMetadata == null || checkIndexMappingUpToDate(event.state());
        final Version mappingVersion = oldestIndexMappingVersion(event.state());
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
            mappingVersion,
            concreteIndexName,
            indexHealth,
            indexState,
            event.state().nodes().getSmallestNonClientNodeVersion(),
            indexUUID
        );
        this.state = newState;

        if (newState.equals(previousState) == false) {
            for (BiConsumer<State, State> listener : stateChangeListeners) {
                listener.accept(previousState, newState);
            }
        }
    }

    public void onStateRecovered(Consumer<State> recoveredStateConsumer) {
        BiConsumer<State, State> stateChangeListener = (previousState, nextState) -> {
            boolean stateJustRecovered = previousState == UNRECOVERED_STATE && nextState != UNRECOVERED_STATE;
            boolean stateAlreadyRecovered = previousState != UNRECOVERED_STATE;
            if (stateJustRecovered) {
                recoveredStateConsumer.accept(nextState);
            } else if (stateAlreadyRecovered) {
                stateChangeListeners.remove(this);
            }
        };
        stateChangeListeners.add(stateChangeListener);
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

    private boolean checkIndexMappingUpToDate(ClusterState clusterState) {
        final Version minimumNonClientNodeVersion = clusterState.nodes().getSmallestNonClientNodeVersion();
        final SystemIndexDescriptor descriptor = systemIndexDescriptor.getDescriptorCompatibleWith(minimumNonClientNodeVersion);
        if (descriptor == null) {
            return false;
        }

        /*
         * The method reference looks wrong here, but it's just counter-intuitive. It expands to:
         *
         *     mappingVersion -> descriptor.getMappingVersion().onOrBefore(mappingVersion)
         *
         * ...which is true if the mappings have been updated.
         */
        return checkIndexMappingVersionMatches(clusterState, descriptor.getMappingsNodeVersion()::onOrBefore);
    }

    private boolean checkIndexMappingVersionMatches(ClusterState clusterState, Predicate<Version> predicate) {
        return checkIndexMappingVersionMatches(this.systemIndexDescriptor.getAliasName(), clusterState, logger, predicate);
    }

    public static boolean checkIndexMappingVersionMatches(
        String indexName,
        ClusterState clusterState,
        Logger logger,
        Predicate<Version> predicate
    ) {
        return loadIndexMappingVersions(indexName, clusterState, logger).stream().allMatch(predicate);
    }

    private Version oldestIndexMappingVersion(ClusterState clusterState) {
        final Set<Version> versions = loadIndexMappingVersions(systemIndexDescriptor.getAliasName(), clusterState, logger);
        return versions.stream().min(Version::compareTo).orElse(null);
    }

    private static Set<Version> loadIndexMappingVersions(String aliasName, ClusterState clusterState, Logger logger) {
        Set<Version> versions = new HashSet<>();
        IndexMetadata indexMetadata = resolveConcreteIndex(aliasName, clusterState.metadata());
        if (indexMetadata != null) {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                versions.add(readMappingVersion(aliasName, mappingMetadata, logger));
            }
        }
        return versions;
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

    private static Version readMappingVersion(String indexName, MappingMetadata mappingMetadata, Logger logger) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
                throw new IllegalStateException("Cannot read security-version string in index " + indexName);
            }
            return Version.fromString((String) meta.get(SECURITY_VERSION_STRING));
        } catch (ElasticsearchParseException e) {
            logger.error(() -> "Cannot parse the mapping for index [" + indexName + "]", e);
            throw new ElasticsearchException("Cannot parse the mapping for index [{}]", e, indexName);
        }
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
                    state.minimumNodeVersion
                );

                if (descriptorForVersion == null) {
                    final String error = systemIndexDescriptor.getMinimumNodeVersionMessage("create index");
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
                    state.minimumNodeVersion
                );
                if (descriptorForVersion == null) {
                    final String error = systemIndexDescriptor.getMinimumNodeVersionMessage("updating mapping");
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
        public static final State UNRECOVERED_STATE = new State(null, false, false, false, false, null, null, null, null, null, null);
        public final Instant creationTime;
        public final boolean isIndexUpToDate;
        public final boolean indexAvailableForSearch;
        public final boolean indexAvailableForWrite;
        public final boolean mappingUpToDate;
        public final Version mappingVersion;
        public final String concreteIndexName;
        public final ClusterHealthStatus indexHealth;
        public final IndexMetadata.State indexState;
        public final Version minimumNodeVersion;
        public final String indexUUID;

        public State(
            Instant creationTime,
            boolean isIndexUpToDate,
            boolean indexAvailableForSearch,
            boolean indexAvailableForWrite,
            boolean mappingUpToDate,
            Version mappingVersion,
            String concreteIndexName,
            ClusterHealthStatus indexHealth,
            IndexMetadata.State indexState,
            Version minimumNodeVersion,
            String indexUUID
        ) {
            this.creationTime = creationTime;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailableForSearch = indexAvailableForSearch;
            this.indexAvailableForWrite = indexAvailableForWrite;
            this.mappingUpToDate = mappingUpToDate;
            this.mappingVersion = mappingVersion;
            this.concreteIndexName = concreteIndexName;
            this.indexHealth = indexHealth;
            this.indexState = indexState;
            this.minimumNodeVersion = minimumNodeVersion;
            this.indexUUID = indexUUID;
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
                && Objects.equals(mappingVersion, state.mappingVersion)
                && Objects.equals(concreteIndexName, state.concreteIndexName)
                && indexHealth == state.indexHealth
                && indexState == state.indexState
                && Objects.equals(minimumNodeVersion, state.minimumNodeVersion);
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
                mappingVersion,
                concreteIndexName,
                indexHealth,
                minimumNodeVersion
            );
        }
    }
}
