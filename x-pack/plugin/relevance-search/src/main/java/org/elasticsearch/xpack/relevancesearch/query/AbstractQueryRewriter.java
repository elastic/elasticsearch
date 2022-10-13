/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.relevancesearch.settings.Settings;
import org.elasticsearch.xpack.relevancesearch.settings.SettingsService;

abstract class AbstractQueryRewriter<S extends Settings> implements QueryRewriter {
    private final ClusterService clusterService;
    private final SettingsService<S> settingsService;

    public AbstractQueryRewriter(ClusterService clusterService, SettingsService<S> settingsService) {
        this.clusterService = clusterService;
        this.settingsService = settingsService;
    }

    protected S getSettings(RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context) {
        String settingsId = getSettingsId(relevanceMatchQuery);
        SearchEngine searchEngine = getSearchEngine(context);

        if (settingsId == null && searchEngine != null) {
            settingsId = getSettingsId(searchEngine);
        }

        if (settingsId != null) {
            try {
                return settingsService.getSettings(settingsId);
            } catch (SettingsService.SettingsServiceException e) {
                throw new IllegalArgumentException("[relevance_match] " + e.getMessage());
            }
        }

        return null;
    }

    protected abstract String getSettingsId(SearchEngine searchEngine);

    protected abstract String getSettingsId(RelevanceMatchQueryBuilder relevanceMatchQuery);

    private SearchEngine getSearchEngine(SearchExecutionContext context) {
        String searchEngineName = context.getSearchEngineName();

        if (searchEngineName == null) {
            return null;
        }

        assert clusterService.state().metadata().searchEngines().containsKey(searchEngineName);

        return clusterService.state().metadata().searchEngines().get(searchEngineName);
    }
}
