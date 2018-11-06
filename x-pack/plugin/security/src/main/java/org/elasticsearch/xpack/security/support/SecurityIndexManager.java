/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.core.upgrade.IndexUpgradeCheckVersion;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Manages the lifecycle of a single index, its template, mapping and and data upgrades/migrations.
 */
public class SecurityIndexManager implements ClusterStateListener {

    public static final String INTERNAL_SECURITY_INDEX = ".security-" + IndexUpgradeCheckVersion.UPRADE_VERSION;
    public static final int INTERNAL_INDEX_FORMAT = 6;
    public static final String SECURITY_VERSION_STRING = "security-version";
    public static final String TEMPLATE_VERSION_PATTERN = Pattern.quote("${security.template.version}");
    public static final String SECURITY_TEMPLATE_NAME = "security-index-template";
    public static final String SECURITY_INDEX_NAME = ".security";
    private static final Logger LOGGER = LogManager.getLogger(SecurityIndexManager.class);

    private final String indexName;
    private final Client client;

    private final List<BiConsumer<State, State>> stateChangeListeners = new CopyOnWriteArrayList<>();

    private volatile State indexState;

    public SecurityIndexManager(Client client, String indexName, ClusterService clusterService) {
        this(client, indexName, new State(false, false, false, false, null, null));
        clusterService.addListener(this);
    }

    private SecurityIndexManager(Client client, String indexName, State indexState) {
        this.client = client;
        this.indexName = indexName;
        this.indexState = indexState;
    }

    public SecurityIndexManager freeze() {
        return new SecurityIndexManager(null, indexName, indexState);
    }

    public static List<String> indexNames() {
        return Collections.unmodifiableList(Arrays.asList(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX));
    }

    public boolean checkMappingVersion(Predicate<Version> requiredVersion) {
        // pull value into local variable for consistent view
        final State currentIndexState = this.indexState;
        return currentIndexState.mappingVersion == null || requiredVersion.test(currentIndexState.mappingVersion);
    }

    public boolean indexExists() {
        return this.indexState.indexExists;
    }

    /**
     * Returns whether the index is on the current format if it exists. If the index does not exist
     * we treat the index as up to date as we expect it to be created with the current format.
     */
    public boolean isIndexUpToDate() {
        return this.indexState.isIndexUpToDate;
    }

    public boolean isAvailable() {
        return this.indexState.indexAvailable;
    }

    public boolean isMappingUpToDate() {
        return this.indexState.mappingUpToDate;
    }

    public ElasticsearchException getUnavailableReason() {
        final State localState = this.indexState;
        if (localState.indexAvailable) {
            throw new IllegalStateException("caller must make sure to use a frozen state and check indexAvailable");
        }

        if (localState.indexExists) {
            return new UnavailableShardsException(null, "at least one primary shard for the security index is unavailable");
        } else {
            return new IndexNotFoundException(SECURITY_INDEX_NAME);
        }
    }

    /**
     * Add a listener for notifications on state changes to the configured index.
     *
     * The previous and current state are provided.
     */
    public void addIndexStateListener(BiConsumer<State, State> listener) {
        stateChangeListeners.add(listener);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think we don't have the
            // .security index but they may not have been restored from the cluster state on disk
            LOGGER.debug("security index manager waiting until state has been recovered");
            return;
        }
        final State previousState = indexState;
        final IndexMetaData indexMetaData = resolveConcreteIndex(indexName, event.state().metaData());
        final boolean indexExists = indexMetaData != null;
        final boolean isIndexUpToDate = indexExists == false ||
            INDEX_FORMAT_SETTING.get(indexMetaData.getSettings()).intValue() == INTERNAL_INDEX_FORMAT;
        final boolean indexAvailable = checkIndexAvailable(event.state());
        final boolean mappingIsUpToDate = indexExists == false || checkIndexMappingUpToDate(event.state());
        final Version mappingVersion = oldestIndexMappingVersion(event.state());
        final ClusterHealthStatus indexStatus = indexMetaData == null ? null :
            new ClusterIndexHealth(indexMetaData, event.state().getRoutingTable().index(indexMetaData.getIndex())).getStatus();
        final State newState = new State(indexExists, isIndexUpToDate, indexAvailable, mappingIsUpToDate, mappingVersion, indexStatus);
        this.indexState = newState;

        if (newState.equals(previousState) == false) {
            for (BiConsumer<State, State> listener : stateChangeListeners) {
                listener.accept(previousState, newState);
            }
        }
    }

    private boolean checkIndexAvailable(ClusterState state) {
        final IndexRoutingTable routingTable = getIndexRoutingTable(state);
        if (routingTable != null && routingTable.allPrimaryShardsActive()) {
            return true;
        }
        LOGGER.debug("Security index [{}] is not yet active", indexName);
        return false;
    }

    /**
     * Returns the routing-table for this index, or <code>null</code> if the index does not exist.
     */
    private IndexRoutingTable getIndexRoutingTable(ClusterState clusterState) {
        IndexMetaData metaData = resolveConcreteIndex(indexName, clusterState.metaData());
        if (metaData == null) {
            return null;
        } else {
            return clusterState.routingTable().index(metaData.getIndex());
        }
    }

    public static boolean checkTemplateExistsAndVersionMatches(
            String templateName, ClusterState state, Logger logger, Predicate<Version> predicate) {

        return TemplateUtils.checkTemplateExistsAndVersionMatches(templateName, SECURITY_VERSION_STRING,
            state, logger, predicate);
    }

    private boolean checkIndexMappingUpToDate(ClusterState clusterState) {
        return checkIndexMappingVersionMatches(clusterState, Version.CURRENT::equals);
    }

    private boolean checkIndexMappingVersionMatches(ClusterState clusterState,
                                                    Predicate<Version> predicate) {
        return checkIndexMappingVersionMatches(indexName, clusterState, LOGGER, predicate);
    }

    public static boolean checkIndexMappingVersionMatches(String indexName,
                                                          ClusterState clusterState, Logger logger,
                                                          Predicate<Version> predicate) {
        return loadIndexMappingVersions(indexName, clusterState, logger)
                .stream().allMatch(predicate);
    }

    private Version oldestIndexMappingVersion(ClusterState clusterState) {
        final Set<Version> versions = loadIndexMappingVersions(indexName, clusterState, LOGGER);
        return versions.stream().min(Version::compareTo).orElse(null);
    }

    private static Set<Version> loadIndexMappingVersions(String indexName,
                                                         ClusterState clusterState, Logger logger) {
        Set<Version> versions = new HashSet<>();
        IndexMetaData indexMetaData = resolveConcreteIndex(indexName, clusterState.metaData());
        if (indexMetaData != null) {
            for (Object object : indexMetaData.getMappings().values().toArray()) {
                MappingMetaData mappingMetaData = (MappingMetaData) object;
                if (mappingMetaData.type().equals(MapperService.DEFAULT_MAPPING)) {
                    continue;
                }
                versions.add(readMappingVersion(indexName, mappingMetaData, logger));
            }
        }
        return versions;
    }

    /**
     * Resolves a concrete index name or alias to a {@link IndexMetaData} instance.  Requires
     * that if supplied with an alias, the alias resolves to at most one concrete index.
     */
    private static IndexMetaData resolveConcreteIndex(final String indexOrAliasName, final MetaData metaData) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(indexOrAliasName);
        if (aliasOrIndex != null) {
            final List<IndexMetaData> indices = aliasOrIndex.getIndices();
            if (aliasOrIndex.isAlias() && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + indexOrAliasName + "] points to more than one index: " +
                        indices.stream().map(imd -> imd.getIndex().getName()).collect(Collectors.toList()));
            }
            return indices.get(0);
        }
        return null;
    }

    private static Version readMappingVersion(String indexName, MappingMetaData mappingMetaData,
                                              Logger logger) {
        try {
            Map<String, Object> meta =
                    (Map<String, Object>) mappingMetaData.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetaData.type(), indexName);
                throw new IllegalStateException("Cannot read security-version string in index " + indexName);
            }
            return Version.fromString((String) meta.get(SECURITY_VERSION_STRING));
        } catch (ElasticsearchParseException e) {
            logger.error(new ParameterizedMessage(
                    "Cannot parse the mapping for index [{}]", indexName), e);
            throw new ElasticsearchException(
                    "Cannot parse the mapping for index [{}]", e, indexName);
        }
    }

    /**
     * Validates the security index is up to date and does not need to migrated. If it is not, the
     * consumer is called with an exception. If the security index is up to date, the runnable will
     * be executed. <b>NOTE:</b> this method does not check the availability of the index; this check
     * is left to the caller so that this condition can be handled appropriately.
     */
    public void checkIndexVersionThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
        final State indexState = this.indexState; // use a local copy so all checks execute against the same state!
        if (indexState.indexExists && indexState.isIndexUpToDate == false) {
            consumer.accept(new IllegalStateException(
                "Security index is not on the current version. Security features relying on the index will not be available until " +
                    "the upgrade API is run on the security index"));
        } else {
            andThen.run();
        }
    }

    /**
     * Prepares the index by creating it if it doesn't exist or updating the mappings if the mappings are
     * out of date. After any tasks have been executed, the runnable is then executed.
     */
    public void prepareIndexIfNeededThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
        final State indexState = this.indexState; // use a local copy so all checks execute against the same state!
        // TODO we should improve this so we don't fire off a bunch of requests to do the same thing (create or update mappings)
        if (indexState.indexExists && indexState.isIndexUpToDate == false) {
            consumer.accept(new IllegalStateException(
                    "Security index is not on the current version. Security features relying on the index will not be available until " +
                            "the upgrade API is run on the security index"));
        } else if (indexState.indexExists == false) {
            Tuple<String, Settings> mappingAndSettings = loadMappingAndSettingsSourceFromTemplate();
            CreateIndexRequest request = new CreateIndexRequest(INTERNAL_SECURITY_INDEX)
                    .alias(new Alias(SECURITY_INDEX_NAME))
                    .mapping("doc", mappingAndSettings.v1(), XContentType.JSON)
                    .waitForActiveShards(ActiveShardCount.ALL)
                    .settings(mappingAndSettings.v2());
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
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
                    }, client.admin().indices()::create);
        } else if (indexState.mappingUpToDate == false) {
            PutMappingRequest request = new PutMappingRequest(INTERNAL_SECURITY_INDEX)
                    .source(loadMappingAndSettingsSourceFromTemplate().v1(), XContentType.JSON)
                    .type("doc");
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                    ActionListener.<AcknowledgedResponse>wrap(putMappingResponse -> {
                        if (putMappingResponse.isAcknowledged()) {
                            andThen.run();
                        } else {
                            consumer.accept(new IllegalStateException("put mapping request was not acknowledged"));
                        }
                    }, consumer), client.admin().indices()::putMapping);
        } else {
            andThen.run();
        }
    }

    private Tuple<String, Settings> loadMappingAndSettingsSourceFromTemplate() {
        final byte[] template = TemplateUtils.loadTemplate("/" + SECURITY_TEMPLATE_NAME + ".json",
                Version.CURRENT.toString(), SecurityIndexManager.TEMPLATE_VERSION_PATTERN).getBytes(StandardCharsets.UTF_8);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(SECURITY_TEMPLATE_NAME).source(template, XContentType.JSON);
        return new Tuple<>(request.mappings().get("doc"), request.settings());
    }

    /**
     * Return true if the state moves from an unhealthy ("RED") index state to a healthy ("non-RED") state.
     */
    public static boolean isMoveFromRedToNonRed(State previousState, State currentState) {
        return (previousState.indexStatus == null || previousState.indexStatus == ClusterHealthStatus.RED)
                && currentState.indexStatus != null && currentState.indexStatus != ClusterHealthStatus.RED;
    }

    /**
     * Return true if the state moves from the index existing to the index not existing.
     */
    public static boolean isIndexDeleted(State previousState, State currentState) {
        return previousState.indexStatus != null && currentState.indexStatus == null;
    }

    /**
     * State of the security index.
     */
    public static class State {
        public final boolean indexExists;
        public final boolean isIndexUpToDate;
        public final boolean indexAvailable;
        public final boolean mappingUpToDate;
        public final Version mappingVersion;
        public final ClusterHealthStatus indexStatus;

        public State(boolean indexExists, boolean isIndexUpToDate, boolean indexAvailable,
                      boolean mappingUpToDate, Version mappingVersion, ClusterHealthStatus indexStatus) {
            this.indexExists = indexExists;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailable = indexAvailable;
            this.mappingUpToDate = mappingUpToDate;
            this.mappingVersion = mappingVersion;
            this.indexStatus = indexStatus;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return indexExists == state.indexExists &&
                isIndexUpToDate == state.isIndexUpToDate &&
                indexAvailable == state.indexAvailable &&
                mappingUpToDate == state.mappingUpToDate &&
                Objects.equals(mappingVersion, state.mappingVersion) &&
                indexStatus == state.indexStatus;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexExists, isIndexUpToDate, indexAvailable, mappingUpToDate, mappingVersion, indexStatus);
        }
    }
}
