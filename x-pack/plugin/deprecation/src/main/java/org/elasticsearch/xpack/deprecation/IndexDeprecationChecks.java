/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecks {

    static DeprecationIssue oldIndicesCheck(IndexMetadata indexMetadata) {
        // TODO: this check needs to be revised. It's trivially true right now.
        Version currentCompatibilityVersion = indexMetadata.getCompatibilityVersion();
        if (currentCompatibilityVersion.before(Version.V_7_0_0)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Old index with a compatibility version < 7.0",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/" + "breaking-changes-8.0.html",
                "This index has version: " + currentCompatibilityVersion,
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue translogRetentionSettingCheck(IndexMetadata indexMetadata) {
        final boolean softDeletesEnabled = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings());
        if (softDeletesEnabled) {
            if (IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexMetadata.getSettings())
                || IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexMetadata.getSettings())) {
                List<String> settingKeys = new ArrayList<>();
                if (IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexMetadata.getSettings())) {
                    settingKeys.add(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey());
                }
                if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexMetadata.getSettings())) {
                    settingKeys.add(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey());
                }
                Map<String, Object> meta = DeprecationIssue.createMetaMapForRemovableSettings(settingKeys);
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "translog retention settings are ignored",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-translog.html",
                    "translog retention settings [index.translog.retention.size] and [index.translog.retention.age] are ignored "
                        + "because translog is no longer used in peer recoveries with soft-deletes enabled (default in 7.0 or later)",
                    false,
                    meta
                );
            }
        }
        return null;
    }

    static DeprecationIssue checkIndexDataPath(IndexMetadata indexMetadata) {
        if (IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexMetadata.getSettings())) {
            final String message = String.format(
                Locale.ROOT,
                "setting [%s] is deprecated and will be removed in a future version",
                IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
            );
            final String url = "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/"
                + "breaking-changes-7.13.html#deprecate-shared-data-path-setting";
            final String details = "Found index data path configured. Discontinue use of this setting.";
            return new DeprecationIssue(DeprecationIssue.Level.WARNING, message, url, details, false, null);
        }
        return null;
    }

    static DeprecationIssue storeTypeSettingCheck(IndexMetadata indexMetadata) {
        final String storeType = IndexModule.INDEX_STORE_TYPE_SETTING.get(indexMetadata.getSettings());
        if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "[simplefs] is deprecated and will be removed in future versions",
                "https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html",
                "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                    + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                    + "as it offers superior or equivalent performance to [simplefs].",
                false,
                null
            );
        }
        return null;
    }

    static DeprecationIssue frozenIndexSettingCheck(IndexMetadata indexMetadata) {
        Boolean isIndexFrozen = FrozenEngine.INDEX_FROZEN.get(indexMetadata.getSettings());
        if (Boolean.TRUE.equals(isIndexFrozen)) {
            String indexName = indexMetadata.getIndex().getName();
            return new DeprecationIssue(
                DeprecationIssue.Level.WARNING,
                "index ["
                    + indexName
                    + "] is a frozen index. The frozen indices feature is deprecated and will be removed in a future version",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/frozen-indices.html",
                "Frozen indices no longer offer any advantages. Consider cold or frozen tiers in place of frozen indices.",
                false,
                null
            );
        }
        return null;
    }
}
