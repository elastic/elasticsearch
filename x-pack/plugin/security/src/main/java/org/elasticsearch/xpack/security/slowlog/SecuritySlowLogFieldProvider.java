/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.slowlog;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

import static org.elasticsearch.index.IndexingSlowLog.INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING;
import static org.elasticsearch.index.SearchSlowLog.INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING;

public class SecuritySlowLogFieldProvider implements SlowLogFieldProvider {
    private final Security plugin;

    private class SecuritySlowLogFields implements SlowLogFields {
        private boolean includeUserInIndexing = false;
        private boolean includeUserInSearch = false;

        SecuritySlowLogFields(IndexSettings indexSettings) {
            indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInSearch = newValue);
            this.includeUserInSearch = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING);
            indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInIndexing = newValue);
            this.includeUserInIndexing = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING);
        }

        SecuritySlowLogFields() {}

        @Override
        public Map<String, String> indexFields() {
            if (includeUserInIndexing) {
                return plugin.getAuthContextForSlowLog();
            }
            return Map.of();
        }

        @Override
        public Map<String, String> searchFields() {
            if (includeUserInSearch) {
                return plugin.getAuthContextForSlowLog();
            }
            return Map.of();
        }

        @Override
        public Map<String, String> queryFields() {
            return plugin.getAuthContextForSlowLog();
        }
    }

    public SecuritySlowLogFieldProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecuritySlowLogFieldProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public SlowLogFields create(IndexSettings indexSettings) {
        return new SecuritySlowLogFields(indexSettings);
    }

    @Override
    public SlowLogFields create() {
        return new SecuritySlowLogFields();
    }
}
