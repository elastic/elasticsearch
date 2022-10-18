/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.relevance;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.relevancesearch.settings.AbstractSettingsService;
import org.elasticsearch.xpack.relevancesearch.settings.index.IndexCreationService;

import java.util.List;
import java.util.Map;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class RelevanceSettingsService extends AbstractSettingsService<RelevanceSettings> {

    public static final String RELEVANCE_SETTINGS_PREFIX = "relevance_settings-";

    @Inject
    public RelevanceSettingsService(final Client client) {
        super(client);
    }

    @Override
    public RelevanceSettings getSettings(String settingsId) throws SettingsNotFoundException, InvalidSettingsException {
        {
            // TODO cache relevance settings, including cache invalidation
            Map<String, Object> settingsContent = null;
            try {
                settingsContent = client.prepareGet(ENT_SEARCH_INDEX, RELEVANCE_SETTINGS_PREFIX + settingsId).get().getSource();
            } catch (IndexNotFoundException e) {
                IndexCreationService.ensureInternalIndex(client);
            }

            if (settingsContent == null) {
                throw new SettingsNotFoundException("Relevance settings " + settingsId + " not found");
            }

            return parseSettings(settingsContent);
        }
    }

    private static RelevanceSettings parseSettings(Map<String, Object> source) throws InvalidSettingsException {

        RelevanceSettings relevanceSettings = new RelevanceSettings();
        QueryConfiguration relevanceSettingsQueryConfiguration = new QueryConfiguration();

        @SuppressWarnings("unchecked")
        final Map<String, Object> queryConfiguration = (Map<String, Object>) source.get("query_configuration");
        if (queryConfiguration == null) {
            throw new InvalidSettingsException(
                "[relevance_match] query configuration not specified in relevance settings. Source: " + source
            );
        }
        @SuppressWarnings("unchecked")
        final List<String> fields = (List<String>) queryConfiguration.get("fields");
        if (fields == null || fields.isEmpty()) {
            throw new InvalidSettingsException("[relevance_match] fields not specified in relevance settings. Source: " + source);
        }
        relevanceSettingsQueryConfiguration.parseFieldsAndBoosts(fields);

        @SuppressWarnings("unchecked")
        final Map<String, List<Map<String, Object>>> scriptScores = (Map<String, List<Map<String, Object>>>) queryConfiguration.get(
            "boosts"
        );
        relevanceSettingsQueryConfiguration.parseScriptScores(scriptScores);

        relevanceSettings.setQueryConfiguration(relevanceSettingsQueryConfiguration);

        return relevanceSettings;
    }
}
