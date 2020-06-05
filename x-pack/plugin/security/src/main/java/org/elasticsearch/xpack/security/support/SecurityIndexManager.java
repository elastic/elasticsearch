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
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Manages the lifecycle, mapping and data upgrades/migrations of the {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS}
 * and {@code RestrictedIndicesNames#SECURITY_MAIN_ALIAS} alias-index pair.
 */
public class SecurityIndexManager implements ClusterStateListener {

    public static final int INTERNAL_MAIN_INDEX_FORMAT = 6;
    public static final int INTERNAL_TOKENS_INDEX_FORMAT = 7;
    public static final String SECURITY_MAIN_TEMPLATE_7 = "security-index-template-7";
    public static final String SECURITY_TOKENS_TEMPLATE_7 = "security-tokens-index-template-7";
    public static final String SECURITY_VERSION_STRING = "security-version";
    public static final String TEMPLATE_VERSION_VARIABLE = "security.template.version";

    private static final Logger logger = LogManager.getLogger(SecurityIndexManager.class);

    private final String aliasName;
    private final String internalIndexName;
    private final int internalIndexFormat;
    private final Supplier<byte[]> mappingSourceSupplier;
    private final Client client;

    private final List<BiConsumer<State, State>> stateChangeListeners = new CopyOnWriteArrayList<>();

    private volatile State indexState;

    public static SecurityIndexManager buildSecurityMainIndexManager(Client client, ClusterService clusterService) {
        return new SecurityIndexManager(client, clusterService, RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
                RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7, INTERNAL_MAIN_INDEX_FORMAT,
                () -> SecurityIndexManager.readTemplateAsBytes(SECURITY_MAIN_TEMPLATE_7));
    }

    public static SecurityIndexManager buildSecurityTokensIndexManager(Client client, ClusterService clusterService) {
        return new SecurityIndexManager(client, clusterService, RestrictedIndicesNames.SECURITY_TOKENS_ALIAS,
                RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7, INTERNAL_TOKENS_INDEX_FORMAT,
                () -> SecurityIndexManager.readTemplateAsBytes(SECURITY_TOKENS_TEMPLATE_7));
    }

    private SecurityIndexManager(Client client, ClusterService clusterService, String aliasName, String internalIndexName,
                                 int internalIndexFormat, Supplier<byte[]> mappingSourceSupplier) {
        this(client, aliasName, internalIndexName, internalIndexFormat, mappingSourceSupplier, State.UNRECOVERED_STATE);
        clusterService.addListener(this);
    }

    // protected for testing
    protected SecurityIndexManager(Client client, String aliasName, String internalIndexName, int internalIndexFormat,
                                 Supplier<byte[]> mappingSourceSupplier, State indexState) {
        this.aliasName = aliasName;
        this.internalIndexName = internalIndexName;
        this.internalIndexFormat = internalIndexFormat;
        this.mappingSourceSupplier = mappingSourceSupplier;
        this.indexState = indexState;
        this.client = client;
    }

    public SecurityIndexManager freeze() {
        return new SecurityIndexManager(null, aliasName, internalIndexName, internalIndexFormat, mappingSourceSupplier, indexState);
    }

    public String aliasName() {
        return aliasName;
    }

    public boolean indexExists() {
        return this.indexState.indexExists();
    }

    public Instant getCreationTime() {
        return this.indexState.creationTime;
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

    public boolean isStateRecovered() {
        return this.indexState != State.UNRECOVERED_STATE;
    }

    public ElasticsearchException getUnavailableReason() {
        final State localState = this.indexState;
        if (localState.indexAvailable) {
            throw new IllegalStateException("caller must make sure to use a frozen state and check indexAvailable");
        }

        if (localState.indexState == IndexMetadata.State.CLOSE) {
            return new IndexClosedException(new Index(localState.concreteIndexName, ClusterState.UNKNOWN_UUID));
        } else if (localState.indexExists()) {
            return new UnavailableShardsException(null,
                "at least one primary shard for the index [" + localState.concreteIndexName + "] is unavailable");
        } else {
            return new IndexNotFoundException(localState.concreteIndexName);
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
        final IndexMetadata indexMetadata = resolveConcreteIndex(aliasName, event.state().metadata());
        final Instant creationTime = indexMetadata != null ? Instant.ofEpochMilli(indexMetadata.getCreationDate()) : null;
        final boolean isIndexUpToDate = indexMetadata == null ||
            INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()).intValue() == internalIndexFormat;
        final boolean indexAvailable = checkIndexAvailable(event.state());
        final boolean mappingIsUpToDate = indexMetadata == null || checkIndexMappingUpToDate(event.state());
        final Version mappingVersion = oldestIndexMappingVersion(event.state());
        final String concreteIndexName = indexMetadata == null ? internalIndexName : indexMetadata.getIndex().getName();
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
        final State newState = new State(creationTime, isIndexUpToDate, indexAvailable, mappingIsUpToDate, mappingVersion,
                concreteIndexName, indexHealth, indexState);
        this.indexState = newState;

        if (newState.equals(previousState) == false) {
            for (BiConsumer<State, State> listener : stateChangeListeners) {
                listener.accept(previousState, newState);
            }
        }
    }

    private boolean checkIndexAvailable(ClusterState state) {
        IndexMetadata metadata = resolveConcreteIndex(aliasName, state.metadata());
        if (metadata == null) {
            logger.debug("Index [{}] is not available - no metadata", aliasName);
            return false;
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn("Index [{}] is closed", aliasName);
            return false;
        }
        final IndexRoutingTable routingTable = state.routingTable().index(metadata.getIndex());
        if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
            logger.debug("Index [{}] is not yet active", aliasName);
            return false;
        } else {
            return true;
        }
    }

    public static boolean checkTemplateExistsAndVersionMatches(String templateName, ClusterState state, Logger logger,
                                                               Predicate<Version> predicate) {
        return TemplateUtils.checkTemplateExistsAndVersionMatches(templateName, SECURITY_VERSION_STRING, state, logger, predicate);
    }

    private boolean checkIndexMappingUpToDate(ClusterState clusterState) {
        return checkIndexMappingVersionMatches(clusterState, Version.CURRENT::equals);
    }

    private boolean checkIndexMappingVersionMatches(ClusterState clusterState, Predicate<Version> predicate) {
        return checkIndexMappingVersionMatches(aliasName, clusterState, logger, predicate);
    }

    public static boolean checkIndexMappingVersionMatches(String indexName, ClusterState clusterState, Logger logger,
                                                          Predicate<Version> predicate) {
        return loadIndexMappingVersions(indexName, clusterState, logger).stream().allMatch(predicate);
    }

    private Version oldestIndexMappingVersion(ClusterState clusterState) {
        final Set<Version> versions = loadIndexMappingVersions(aliasName, clusterState, logger);
        return versions.stream().min(Version::compareTo).orElse(null);
    }

    private static Set<Version> loadIndexMappingVersions(String aliasName, ClusterState clusterState, Logger logger) {
        Set<Version> versions = new HashSet<>();
        IndexMetadata indexMetadata = resolveConcreteIndex(aliasName, clusterState.metadata());
        if (indexMetadata != null) {
            MappingMetadata mmd = indexMetadata.mapping();
            if (mmd != null) {
                versions.add(readMappingVersion(aliasName, mmd, logger));
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
            final List<IndexMetadata> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException("Alias [" + indexOrAliasName + "] points to more than one index: " +
                        indices.stream().map(imd -> imd.getIndex().getName()).collect(Collectors.toList()));
            }
            return indices.get(0);
        }
        return null;
    }

    private static Version readMappingVersion(String indexName, MappingMetadata mappingMetadata, Logger logger) {
        try {
            Map<String, Object> meta =
                    (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
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
     * Validates that the index is up to date and does not need to be migrated. If it is not, the
     * consumer is called with an exception. If the index is up to date, the runnable will
     * be executed. <b>NOTE:</b> this method does not check the availability of the index; this check
     * is left to the caller so that this condition can be handled appropriately.
     */
    public void checkIndexVersionThenExecute(final Consumer<Exception> consumer, final Runnable andThen) {
        final State indexState = this.indexState; // use a local copy so all checks execute against the same state!
        if (indexState.indexExists() && indexState.isIndexUpToDate == false) {
            consumer.accept(new IllegalStateException(
                    "Index [" + indexState.concreteIndexName + "] is not on the current version. Security features relying on the index"
                            + " will not be available until the upgrade API is run on the index"));
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
        try {
            // TODO we should improve this so we don't fire off a bunch of requests to do the same thing (create or update mappings)
            if (indexState == State.UNRECOVERED_STATE) {
                throw new ElasticsearchStatusException(
                        "Cluster state has not been recovered yet, cannot write to the [" + indexState.concreteIndexName + "] index",
                        RestStatus.SERVICE_UNAVAILABLE);
            } else if (indexState.indexExists() && indexState.isIndexUpToDate == false) {
                throw new IllegalStateException("Index [" + indexState.concreteIndexName + "] is not on the current version."
                        + "Security features relying on the index will not be available until the upgrade API is run on the index");
            } else if (indexState.indexExists() == false) {
                assert indexState.concreteIndexName != null;
                logger.info("security index does not exist. Creating [{}] with alias [{}]", indexState.concreteIndexName, this.aliasName);
                final byte[] mappingSource = mappingSourceSupplier.get();
                final Tuple<String, Settings> mappingAndSettings = parseMappingAndSettingsFromTemplateBytes(mappingSource);
                CreateIndexRequest request = new CreateIndexRequest(indexState.concreteIndexName)
                        .alias(new Alias(this.aliasName))
                        .mapping(mappingAndSettings.v1())
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
                logger.info("Index [{}] (alias [{}]) is not up to date. Updating mapping", indexState.concreteIndexName, this.aliasName);
                final byte[] mappingSource = mappingSourceSupplier.get();
                final Tuple<String, Settings> mappingAndSettings = parseMappingAndSettingsFromTemplateBytes(mappingSource);
                PutMappingRequest request = new PutMappingRequest(indexState.concreteIndexName)
                        .source(mappingAndSettings.v1(), XContentType.JSON);
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
        } catch (Exception e) {
            consumer.accept(e);
        }
    }

    /**
     * Return true if the state moves from an unhealthy ("RED") index state to a healthy ("non-RED") state.
     */
    public static boolean isMoveFromRedToNonRed(State previousState, State currentState) {
        return (previousState.indexHealth == null || previousState.indexHealth == ClusterHealthStatus.RED)
                && currentState.indexHealth != null && currentState.indexHealth != ClusterHealthStatus.RED;
    }

    /**
     * Return true if the state moves from the index existing to the index not existing.
     */
    public static boolean isIndexDeleted(State previousState, State currentState) {
        return previousState.indexHealth != null && currentState.indexHealth == null;
    }

    private static byte[] readTemplateAsBytes(String templateName) {
        return TemplateUtils.loadTemplate("/" + templateName + ".json", Version.CURRENT.toString(),
                SecurityIndexManager.TEMPLATE_VERSION_VARIABLE).getBytes(StandardCharsets.UTF_8);
    }

    private static Tuple<String, Settings> parseMappingAndSettingsFromTemplateBytes(byte[] template) throws IOException {
        final PutIndexTemplateRequest request = new PutIndexTemplateRequest("name_is_not_important").source(template, XContentType.JSON);
        final String mappingSource = request.mappings();
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation); // {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.generator().copyCurrentStructure(parser);
            return new Tuple<>(Strings.toString(builder), request.settings());
        }
    }

    /**
     * State of the security index.
     */
    public static class State {
        public static final State UNRECOVERED_STATE = new State(null, false, false, false, null, null, null, null);
        public final Instant creationTime;
        public final boolean isIndexUpToDate;
        public final boolean indexAvailable;
        public final boolean mappingUpToDate;
        public final Version mappingVersion;
        public final String concreteIndexName;
        public final ClusterHealthStatus indexHealth;
        public final IndexMetadata.State indexState;

        public State(Instant creationTime, boolean isIndexUpToDate, boolean indexAvailable,
                     boolean mappingUpToDate, Version mappingVersion, String concreteIndexName, ClusterHealthStatus indexHealth,
                     IndexMetadata.State indexState) {
            this.creationTime = creationTime;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailable = indexAvailable;
            this.mappingUpToDate = mappingUpToDate;
            this.mappingVersion = mappingVersion;
            this.concreteIndexName = concreteIndexName;
            this.indexHealth = indexHealth;
            this.indexState = indexState;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return Objects.equals(creationTime, state.creationTime) &&
                isIndexUpToDate == state.isIndexUpToDate &&
                indexAvailable == state.indexAvailable &&
                mappingUpToDate == state.mappingUpToDate &&
                Objects.equals(mappingVersion, state.mappingVersion) &&
                Objects.equals(concreteIndexName, state.concreteIndexName) &&
                indexHealth == state.indexHealth &&
                indexState == state.indexState;
        }

        public boolean indexExists() {
            return creationTime != null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(creationTime, isIndexUpToDate, indexAvailable, mappingUpToDate, mappingVersion, concreteIndexName,
                indexHealth);
        }
    }
}
