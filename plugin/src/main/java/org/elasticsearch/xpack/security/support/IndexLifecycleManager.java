/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.elasticsearch.xpack.upgrade.IndexUpgradeCheck;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_FORMAT_SETTING;
import static org.elasticsearch.xpack.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;

/**
 * Manages the lifecycle of a single index, its template, mapping and and data upgrades/migrations.
 */
public class IndexLifecycleManager extends AbstractComponent {

    public static final String INTERNAL_SECURITY_INDEX = ".security-" + IndexUpgradeCheck.UPRADE_VERSION;
    public static final int INTERNAL_INDEX_FORMAT = 6;
    public static final String SECURITY_VERSION_STRING = "security-version";
    public static final String TEMPLATE_VERSION_PATTERN =
            Pattern.quote("${security.template.version}");

    private final String indexName;
    private final String templateName;
    private final Client client;

    private final List<BiConsumer<ClusterIndexHealth, ClusterIndexHealth>> indexHealthChangeListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Boolean, Boolean>> indexOutOfDateListeners = new CopyOnWriteArrayList<>();

    private volatile State indexState = new State(false, false, false, false, null);

    public IndexLifecycleManager(Settings settings, Client client, String indexName, String templateName) {
        super(settings);
        this.client = client;
        this.indexName = indexName;
        this.templateName = templateName;
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

    public boolean isWritable() {
        return this.indexState.canWriteToIndex;
    }

    /**
     * Adds a listener which will be notified when the security index health changes. The previous and
     * current health will be provided to the listener so that the listener can determine if any action
     * needs to be taken.
     */
    public void addIndexHealthChangeListener(BiConsumer<ClusterIndexHealth, ClusterIndexHealth> listener) {
        indexHealthChangeListeners.add(listener);
    }

    /**
     * Adds a listener which will be notified when the security index out of date value changes. The previous and
     * current value will be provided to the listener so that the listener can determine if any action
     * needs to be taken.
     */
    public void addIndexOutOfDateListener(BiConsumer<Boolean, Boolean> listener) {
        indexOutOfDateListeners.add(listener);
    }

    public void clusterChanged(ClusterChangedEvent event) {
        final boolean previousUpToDate = this.indexState.isIndexUpToDate;
        processClusterState(event.state());
        checkIndexHealthChange(event);
        if (previousUpToDate != this.indexState.isIndexUpToDate) {
            notifyIndexOutOfDateListeners(previousUpToDate, this.indexState.isIndexUpToDate);
        }
    }

    private void processClusterState(ClusterState clusterState) {
        assert clusterState != null;
        final IndexMetaData securityIndex = resolveConcreteIndex(indexName, clusterState.metaData());
        final boolean indexExists = securityIndex != null;
        final boolean isIndexUpToDate = indexExists == false ||
                INDEX_FORMAT_SETTING.get(securityIndex.getSettings()).intValue() == INTERNAL_INDEX_FORMAT;
        final boolean indexAvailable = checkIndexAvailable(clusterState);
        final boolean templateIsUpToDate = TemplateUtils.checkTemplateExistsAndIsUpToDate(templateName,
            SECURITY_VERSION_STRING, clusterState, logger);
        final boolean mappingIsUpToDate = checkIndexMappingUpToDate(clusterState);
        final boolean canWriteToIndex = templateIsUpToDate && (mappingIsUpToDate || isIndexUpToDate);
        final Version mappingVersion = oldestIndexMappingVersion(clusterState);
        this.indexState = new State(indexExists, isIndexUpToDate, indexAvailable, canWriteToIndex, mappingVersion);
    }

    private void checkIndexHealthChange(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final ClusterState previousState = event.previousState();
        final IndexMetaData indexMetaData = resolveConcreteIndex(indexName, state.metaData());
        final IndexMetaData previousIndexMetaData = resolveConcreteIndex(indexName, previousState.metaData());
        if (indexMetaData != null) {
            final ClusterIndexHealth currentHealth =
                    new ClusterIndexHealth(indexMetaData, state.getRoutingTable().index(indexMetaData.getIndex()));
            final ClusterIndexHealth previousHealth = previousIndexMetaData != null ? new ClusterIndexHealth(previousIndexMetaData,
                            previousState.getRoutingTable().index(previousIndexMetaData.getIndex())) : null;

            if (previousHealth == null || previousHealth.getStatus() != currentHealth.getStatus()) {
                notifyIndexHealthChangeListeners(previousHealth, currentHealth);
            }
        } else if (previousIndexMetaData != null) {
            final ClusterIndexHealth previousHealth =
                    new ClusterIndexHealth(previousIndexMetaData, previousState.getRoutingTable().index(previousIndexMetaData.getIndex()));
            notifyIndexHealthChangeListeners(previousHealth, null);
        }
    }

    private void notifyIndexHealthChangeListeners(ClusterIndexHealth previousHealth, ClusterIndexHealth currentHealth) {
        for (BiConsumer<ClusterIndexHealth, ClusterIndexHealth> consumer : indexHealthChangeListeners) {
            try {
                consumer.accept(previousHealth, currentHealth);
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("failed to notify listener [{}] of index health change", consumer), e);
            }
        }
    }

    private void notifyIndexOutOfDateListeners(boolean previous, boolean current) {
        for (BiConsumer<Boolean, Boolean> consumer : indexOutOfDateListeners) {
            try {
                consumer.accept(previous, current);
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("failed to notify listener [{}] of index out of date change", consumer), e);
            }
        }
    }

    private boolean checkIndexAvailable(ClusterState state) {
        final IndexRoutingTable routingTable = getIndexRoutingTable(state);
        if (routingTable != null && routingTable.allPrimaryShardsActive()) {
            return true;
        }
        logger.debug("Security index [{}] is not yet active", indexName);
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
        return checkIndexMappingVersionMatches(indexName, clusterState, logger, predicate);
    }

    public static boolean checkIndexMappingVersionMatches(String indexName,
                                                          ClusterState clusterState, Logger logger,
                                                          Predicate<Version> predicate) {
        return loadIndexMappingVersions(indexName, clusterState, logger)
                .stream().allMatch(predicate);
    }

    private Version oldestIndexMappingVersion(ClusterState clusterState) {
        final Set<Version> versions = loadIndexMappingVersions(indexName, clusterState, logger);
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
     * Creates the security index, if it does not already exist, then runs the given
     * action on the security index.
     */
    public <T> void createIndexIfNeededThenExecute(final ActionListener<T> listener, final Runnable andThen) {
        if (this.indexState.indexExists) {
            andThen.run();
        } else {
            CreateIndexRequest request = new CreateIndexRequest(INTERNAL_SECURITY_INDEX);
            request.alias(new Alias(SECURITY_INDEX_NAME));
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                    new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            if (createIndexResponse.isAcknowledged()) {
                                andThen.run();
                            } else {
                                listener.onFailure(new ElasticsearchException("Failed to create security index"));
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
                                listener.onFailure(e);
                            }
                        }
                    }, client.admin().indices()::create);
        }
    }

    /**
     * Holder class so we can update all values at once
     */
    private static class State {
        private final boolean indexExists;
        private final boolean isIndexUpToDate;
        private final boolean indexAvailable;
        private final boolean canWriteToIndex;
        private final Version mappingVersion;

        private State(boolean indexExists, boolean isIndexUpToDate, boolean indexAvailable,
                      boolean canWriteToIndex, Version mappingVersion) {
            this.indexExists = indexExists;
            this.isIndexUpToDate = isIndexUpToDate;
            this.indexAvailable = indexAvailable;
            this.canWriteToIndex = canWriteToIndex;
            this.mappingVersion = mappingVersion;
        }
    }
}
