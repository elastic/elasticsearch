/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.logging;

import org.elasticsearch.index.ActionLogFieldProvider;
import org.elasticsearch.index.ActionLogFields;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

import static org.elasticsearch.index.IndexingSlowLog.INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING;
import static org.elasticsearch.index.SearchSlowLog.INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING;

public class SecurityActionLogFieldProvider implements ActionLogFieldProvider {
    private final Security plugin;

    private class SecurityActionLogFields implements ActionLogFields {
        private boolean includeUserInIndexing = false;
        private boolean includeUserInSearch = false;

        SecurityActionLogFields(IndexSettings indexSettings) {
            indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInSearch = newValue);
            this.includeUserInSearch = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_INCLUDE_USER_SETTING);
            indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING, newValue -> this.includeUserInIndexing = newValue);
            this.includeUserInIndexing = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_INCLUDE_USER_SETTING);
        }

        SecurityActionLogFields() {}

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

    public SecurityActionLogFieldProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecurityActionLogFieldProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public ActionLogFields create(IndexSettings indexSettings) {
        return new SecurityActionLogFields(indexSettings);
    }

    @Override
    public ActionLogFields create() {
        return new SecurityActionLogFields();
    }
}
