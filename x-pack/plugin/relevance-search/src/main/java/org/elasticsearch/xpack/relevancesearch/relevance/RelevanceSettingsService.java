/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.RELEVANCE_SETTINGS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class RelevanceSettingsService implements ClusterStateListener {

    public static final String ENT_SEARCH_INDEX = ".ent-search";
    public static final String RELEVANCE_SETTINGS_PREFIX = "relevance_settings-";
    private final Client client;
    private final ClusterService clusterService;

    private static final Logger logger = LogManager.getLogger(RelevanceSettingsService.class);

    @Inject
    public RelevanceSettingsService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    public RelevanceSettings getRelevanceSettings(String settingsId) throws RelevanceSettingsNotFoundException,
        RelevanceSettingsInvalidException {
        // TODO cache relevance settings, including cache invalidation
        Map<String, Object> settingsContent = null;
        try {
            settingsContent = client.prepareGet(ENT_SEARCH_INDEX, RELEVANCE_SETTINGS_PREFIX + settingsId).get().getSource();
        } catch (IndexNotFoundException e) {
            ensureInternalIndex(client);
        }

        if (settingsContent == null) {
            throw new RelevanceSettingsNotFoundException("Relevance settings " + settingsId + " not found");
        }

        return parseRelevanceSettings(settingsContent);
    }

    private RelevanceSettings parseRelevanceSettings(Map<String, Object> source) throws RelevanceSettingsInvalidException {

        RelevanceSettings relevanceSettings = new RelevanceSettings();
        QueryConfiguration relevanceSettingsQueryConfiguration = new QueryConfiguration();

        @SuppressWarnings("unchecked")
        final Map<String, Object> queryConfiguration = (Map<String, Object>) source.get("query_configuration");
        if (queryConfiguration == null) {
            throw new RelevanceSettingsInvalidException(
                "[relevance_match] query configuration not specified in relevance settings. Source: " + source
            );
        }
        @SuppressWarnings("unchecked")
        final List<String> fields = (List<String>) queryConfiguration.get("fields");
        if (fields == null || fields.isEmpty()) {
            throw new RelevanceSettingsInvalidException("[relevance_match] fields not specified in relevance settings. Source: " + source);
        }
        @SuppressWarnings("unchecked")
        final Map<String, List<Map<String, Object>>> scriptScores = (Map<String, List<Map<String, Object>>>) queryConfiguration.get(
            "boosts"
        );

        relevanceSettingsQueryConfiguration.parseFieldsAndBoosts(fields);
        relevanceSettingsQueryConfiguration.parseScriptScores(scriptScores);

        relevanceSettings.setQueryConfiguration(relevanceSettingsQueryConfiguration);

        return relevanceSettings;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }

        ensureInternalIndex(this.client);
        this.clusterService.removeListener(this);
    }

    public static class RelevanceSettingsNotFoundException extends Exception {
        public RelevanceSettingsNotFoundException(String message) {
            super(message);
        }
    }

    public static class RelevanceSettingsInvalidException extends Exception {
        public RelevanceSettingsInvalidException(String message) {
            super(message);
        }
    }

    private static void ensureInternalIndex(Client client) {
        CreateIndexRequest request = new CreateIndexRequest(ENT_SEARCH_INDEX).mapping(getInternalIndexMapping())
            .settings(getInternalIndexSettings())
            .origin(RELEVANCE_SETTINGS_ORIGIN);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            RELEVANCE_SETTINGS_ORIGIN,
            request,
            new ActionListener<CreateIndexResponse>() {
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    logger.info("Created " + ENT_SEARCH_INDEX + " index.");
                }

                public void onFailure(Exception e) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        logger.info("Index " + ENT_SEARCH_INDEX + " already exists.");
                    } else {
                        logger.info("Failed to create " + ENT_SEARCH_INDEX + " index " + e.toString());
                    }
                }
            },
            client.admin().indices()::create
        );
    }

    private static Settings getInternalIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .build();
    }

    private static XContentBuilder getInternalIndexMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .endObject()
                .startArray("dynamic_templates")
                .startObject()
                .startObject("string_as_keyword")
                .field("match_mapping_type", "string")
                .startObject("mapping")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("type")
                .field("type", "keyword")
                .endObject()
                .startObject("query_type")
                .field("type", "keyword")
                .endObject()
                .startObject("query_configuration")
                .startObject("properties")
                .startObject("fields")
                .field("type", "keyword")
                .endObject()
                .startObject("boosts")
                .field("type", "object")
                .field("dynamic", "true")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + ENT_SEARCH_INDEX, e);
        }
    }
}
