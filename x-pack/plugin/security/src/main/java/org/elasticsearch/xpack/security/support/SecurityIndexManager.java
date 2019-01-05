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
 * Manages the lifecycle of a single index, its mapping and and data upgrades/migrations.
 */
public class SecurityIndexManager implements ClusterStateListener {

    public static final String SECURITY_ALIAS_NAME = ".security";
    public static final int INTERNAL_SECURITY_INDEX_FORMAT = 6;
    public static final String INTERNAL_SECURITY_INDEX = ".security-" + INTERNAL_SECURITY_INDEX_FORMAT;
    public static final String SECURITY_TEMPLATE_NAME = "security-index-template";

    public static final String SECURITY_TOKENS_ALIAS_NAME = ".security-tokens";
    public static final int INTERNAL_SECURITY_TOKENS_INDEX_FORMAT = 7;
    public static final String INTERNAL_SECURITY_TOKENS_INDEX = ".security-tokens-" + INTERNAL_SECURITY_TOKENS_INDEX_FORMAT;
    public static final String SECURITY_TOKENS_TEMPLATE_NAME = "security-tokens-index-template";

    public static final String SECURITY_VERSION_STRING = "security-version";
    public static final String TEMPLATE_VERSION_PATTERN = Pattern.quote("${security.template.version}");

    private static final Logger logger = LogManager.getLogger();

    private final String indexName;
    private final int expectedIndexFormat;
    private final String aliasName;
    private final String templateName;
    private final Client client;

    private final List<BiConsumer<State, State>> stateChangeListeners = new CopyOnWriteArrayList<>();

    private volatile State indexState;

    public static SecurityIndexManager buildSecurityIndexManager(Client client, ClusterService clusterService) {
        return new SecurityIndexManager(client, INTERNAL_SECURITY_INDEX, INTERNAL_SECURITY_INDEX_FORMAT, SECURITY_ALIAS_NAME,
                SECURITY_TEMPLATE_NAME, clusterService);
    }

    public static SecurityIndexManager buildSecurityTokensIndexManager(Client client, ClusterService clusterService) {
        return new SecurityIndexManager(client, INTERNAL_SECURITY_TOKENS_INDEX, INTERNAL_SECURITY_TOKENS_INDEX_FORMAT,
                SECURITY_TOKENS_ALIAS_NAME, SECURITY_TOKENS_TEMPLATE_NAME, clusterService);
    }

    public SecurityIndexManager(Client client, String indexName, int expectedIndexFormat, String aliasName, String templateName,
            ClusterService clusterService) {
        this(client, indexName, expectedIndexFormat, aliasName, templateName, new State(false, false, false, false, null, null));
        clusterService.addListener(this);
    }

    private SecurityIndexManager(Client client, String indexName, int expectedIndexFormat, String aliasName, String templateName,
            State indexState) {
        this.client = client;
        this.indexName = indexName;
        this.expectedIndexFormat = expectedIndexFormat;
        this.aliasName = aliasName;
        this.templateName = templateName;
        this.indexState = indexState;
    }

    public SecurityIndexManager freeze() {
        return new SecurityIndexManager(null, indexName, expectedIndexFormat, aliasName, templateName, indexState);
    }

    public static List<String> indexNames() {
        return Collections.unmodifiableList(Arrays.asList(SECURITY_ALIAS_NAME, INTERNAL_SECURITY_INDEX));
    }

    public boolean checkMappingVersion(Predicate<Version> requiredVersion) {
        // pull value into local variable for consistent view
        final State currentIndexState = this.indexState;
        return currentIndexState.mappingVersion == null || requiredVersion.test(currentIndexState.mappingVersion);
    }

    public String aliasName() {
        return aliasName;
    }

    public boolean exists() {
        return this.indexState.exists;
    }

    /**
     * Returns whether the index is on the current format if it exists. If the index does not exist
     * we treat the index as up to date as we expect it to be created with the current format.
     */
    public boolean isUpToDate() {
        return this.indexState.isUpToDate;
    }

    /**
     * Returns whether all primary shards are assigned.
     */
    public boolean isAvailable() {
        return this.indexState.available;
    }

    /**
     * Returns whether the index has the mapping Mapping can change at any version.
     */
    public boolean isMappingUpToDate() {
        return this.indexState.mappingUpToDate;
    }

    public ElasticsearchException getUnavailableReason() {
        final State localState = this.indexState;
        if (localState.available) {
            throw new IllegalStateException("caller must make sure to use a frozen state and check isAvailable [" + indexName + "]");
        }

        if (localState.exists) {
            return new UnavailableShardsException(null, "at least one primary shard for the index [" + indexName + "] is unavailable");
        } else {
            return new IndexNotFoundException(indexName);
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
            logger.debug("security index manager waiting until state has been recovered");
            return;
        }
        final State previousState = indexState;
        // check for the index starting with the alias
        final IndexMetaData indexMetaData = resolveConcreteIndex(aliasName, event.state().metaData());
        final boolean indexExists = indexMetaData != null;
        // if index does not exist it will be created with an up to date version
        // if the version is newer that the latest expected, then a manual upgrade has been issued 
        final boolean isIndexUpToDate = indexExists == false
                || INDEX_FORMAT_SETTING.get(indexMetaData.getSettings()).intValue() >= expectedIndexFormat;
        // all primary shards allocated
        final boolean indexAvailable = checkIndexAvailable(event.state());
        // mapping can change in any version, unlike the "index.format"
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
        logger.debug("Index [{}] is not yet active", indexName);
        return false;
    }

    /**
     * Returns the routing-table for this index, or <code>null</code> if the index does not exist.
     */
    private IndexRoutingTable getIndexRoutingTable(ClusterState clusterState) {
        IndexMetaData metaData = resolveConcreteIndex(aliasName, clusterState.metaData());
        if (metaData == null) {
            return null;
        } else {
            return clusterState.routingTable().index(metaData.getIndex());
        }
    }

    private boolean checkIndexMappingUpToDate(ClusterState clusterState) {
        return checkIndexMappingVersionMatches(aliasName, clusterState, Version.CURRENT::equals);
    }

    // public and static for testing
    public static boolean checkIndexMappingVersionMatches(String aliasName, ClusterState clusterState, Predicate<Version> predicate) {
        return loadIndexMappingVersions(aliasName, clusterState, logger).stream().allMatch(predicate);
    }

    private Version oldestIndexMappingVersion(ClusterState clusterState) {
        final Set<Version> versions = loadIndexMappingVersions(aliasName, clusterState, logger);
        return versions.stream().min(Version::compareTo).orElse(null);
    }

    private static Set<Version> loadIndexMappingVersions(String aliasName, ClusterState clusterState, Logger logger) {
        Set<Version> versions = new HashSet<>();
        IndexMetaData indexMetaData = resolveConcreteIndex(aliasName, clusterState.metaData());
        if (indexMetaData != null) {
            for (Object object : indexMetaData.getMappings().values().toArray()) {
                MappingMetaData mappingMetaData = (MappingMetaData) object;
                if (mappingMetaData.type().equals(MapperService.DEFAULT_MAPPING)) {
                    continue;
                }
                versions.add(readMappingVersion(indexMetaData.getIndex().getName(), mappingMetaData, logger));
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

    private static Version readMappingVersion(String indexName, MappingMetaData mappingMetaData, Logger logger) {
        try {
            Map<String, Object> meta = (Map<String, Object>) mappingMetaData.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetaData.type(), indexName);
                throw new IllegalStateException("Cannot read security-version string in index " + indexName);
            }
            return Version.fromString((String) meta.get(SECURITY_VERSION_STRING));
        } catch (ElasticsearchParseException e) {
            logger.error(new ParameterizedMessage("Cannot parse the mapping for index [{}]", indexName), e);
            throw new ElasticsearchException("Cannot parse the mapping for index [{}]", e, indexName);
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
        if (indexState.exists && indexState.isUpToDate == false) {
            consumer.accept(new IllegalStateException("Index [" + indexName + "] is not on the current version. " +
                    "Security features relying on the index will not be available until the upgrade API is run on the security index."));
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
        if (indexState.exists && indexState.isUpToDate == false) {
            consumer.accept(new IllegalStateException("Index [" + indexName + "] is not on the current version. " +
                    "Security features relying on the index will not be available until the upgrade API is run on the security index"));
        } else if (indexState.exists == false) {
            Tuple<String, Settings> mappingAndSettings = loadMappingAndSettingsSourceFromTemplate();
            CreateIndexRequest request = new CreateIndexRequest(indexName)
                    .alias(new Alias(aliasName))
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
            PutMappingRequest request = new PutMappingRequest(indexName)
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
        final byte[] template = TemplateUtils.loadTemplate("/" + templateName + ".json",
                Version.CURRENT.toString(), SecurityIndexManager.TEMPLATE_VERSION_PATTERN).getBytes(StandardCharsets.UTF_8);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(template, XContentType.JSON);
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
        public final boolean exists;
        public final boolean isUpToDate;
        public final boolean available;
        public final boolean mappingUpToDate;
        public final Version mappingVersion;
        public final ClusterHealthStatus indexStatus;

        public State(boolean indexExists, boolean isIndexUpToDate, boolean indexAvailable, boolean mappingUpToDate, Version mappingVersion,
                ClusterHealthStatus indexStatus) {
            this.exists = indexExists;
            this.isUpToDate = isIndexUpToDate;
            this.available = indexAvailable;
            this.mappingUpToDate = mappingUpToDate;
            this.mappingVersion = mappingVersion;
            this.indexStatus = indexStatus;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return exists == state.exists &&
                isUpToDate == state.isUpToDate &&
                available == state.available &&
                mappingUpToDate == state.mappingUpToDate &&
                Objects.equals(mappingVersion, state.mappingVersion) &&
                indexStatus == state.indexStatus;
        }

        @Override
        public int hashCode() {
            return Objects.hash(exists, isUpToDate, available, mappingUpToDate, mappingVersion, indexStatus);
        }
    }
}
