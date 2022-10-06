/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.settings;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.relevancesearch.relevance.QueryConfiguration;

import java.util.List;
import java.util.Map;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class RelevanceSettingsService {

    public static final String ENT_SEARCH_INDEX = ".ent-search";
    public static final String RELEVANCE_SETTINGS_PREFIX = "relevance_settings-";
    private final Client client;

    public RelevanceSettingsService(final Client client) {
        this.client = client;
    }

    public RelevanceSettings getRelevanceSettings(String settingsId) throws RelevanceSettingsNotFoundException,
        RelevanceSettingsInvalidException {
        // TODO cache relevance settings, including cache invalidation
        final Map<String, Object> settingsContent = client.prepareGet(ENT_SEARCH_INDEX, RELEVANCE_SETTINGS_PREFIX + settingsId)
            .get()
            .getSource();

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
        relevanceSettingsQueryConfiguration.parseFieldsAndBoosts(fields);

        relevanceSettings.setQueryConfiguration(relevanceSettingsQueryConfiguration);

        return relevanceSettings;
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
}
