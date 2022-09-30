/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.client.internal.Client;

import java.util.List;
import java.util.Map;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class RelevanceSettingsService {

    public static final String ENT_SEARCH_INDEX = ".ent-search";
    public static final String RELEVANCE_SETTINGS_PREFIX = "ent-search-settings-";
    private final Client client;

    public RelevanceSettingsService(final Client client) {
        this.client = client;
    }

    public RelevanceSettings getRelevanceSettings(String settingsId) throws RelevanceSettingsNotFoundExecption {
        // TODO cache relevance settings, including cache invalidation
        final Map<String, Object> settingsContent = client.prepareGet(ENT_SEARCH_INDEX, RELEVANCE_SETTINGS_PREFIX + settingsId)
            .get()
            .getSource();

        if (settingsContent == null) {
            throw new RelevanceSettingsNotFoundExecption(String.format("Relevance settings %s not found", settingsId));
        }

        return parseRelevanceSettings(settingsContent);
    }

    private RelevanceSettings parseRelevanceSettings(Map<String, Object> source) {

        RelevanceSettings relevanceSettings = new RelevanceSettings();

        @SuppressWarnings("unchecked")
        final List<String> fields = (List<String>) source.get("fields");
        relevanceSettings.setFields(fields);

        return relevanceSettings;
    }

    public static class RelevanceSettingsNotFoundExecption extends Exception {
        public RelevanceSettingsNotFoundExecption(String message) {
            super(message);
        }
    }
}
