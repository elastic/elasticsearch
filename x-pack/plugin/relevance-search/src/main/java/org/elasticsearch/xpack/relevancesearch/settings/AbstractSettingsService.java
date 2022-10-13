/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.relevancesearch.settings.index.IndexCreationService;

import java.util.Map;

public abstract class AbstractSettingsService<S extends Settings> implements SettingsService<S> {
    public static final String ENT_SEARCH_INDEX = ".ent-search";

    protected final Client client;

    public AbstractSettingsService(final Client client) {
        this.client = client;
    }

    @Override
    public S getSettings(String settingsId) throws SettingsNotFoundException, InvalidSettingsException {
        // TODO cache relevance settings, including cache invalidation
        Map<String, Object> settingsContent = null;
        try {
            settingsContent = client.prepareGet(ENT_SEARCH_INDEX, getSettingsPrefix() + settingsId).get().getSource();
        } catch (IndexNotFoundException e) {
            IndexCreationService.ensureInternalIndex(client);
        }

        if (settingsContent == null) {
            throw new SettingsNotFoundException(getName() + " settings " + settingsId + " not found");
        }

        return parseSettings(settingsContent);
    }

    protected abstract String getName();

    protected abstract S parseSettings(Map<String, Object> source) throws InvalidSettingsException;

    protected abstract String getSettingsPrefix();

}
