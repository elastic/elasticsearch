/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.slowlog;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

import static org.elasticsearch.index.IndexingSlowLog.INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING;
import static org.elasticsearch.index.SearchSlowLog.INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING;

public class SecuritySlowLogFieldProvider implements SlowLogFieldProvider {
    private final Security plugin;
    private boolean includeUserInIndexing = false;
    private boolean includeUserInSearch = false;

    public SecuritySlowLogFieldProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecuritySlowLogFieldProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public void init(IndexSettings indexSettings) {
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInSearch = newValue);
        this.includeUserInSearch = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING);
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInIndexing = newValue);
        this.includeUserInIndexing = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING);
    }

    @Override
    public Map<String, String> indexSlowLogFields() {
        if (includeUserInIndexing) {
            return plugin.getAuthContextForSlowLog();
        }
        return Map.of();
    }

    @Override
    public Map<String, String> searchSlowLogFields() {
        if (includeUserInSearch) {
            return plugin.getAuthContextForSlowLog();
        }
        return Map.of();
    }
}
