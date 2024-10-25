/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

final class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    static final Setting<Boolean> CLUSTER_LOGSDB_ENABLED = Setting.boolSetting(
        "cluster.logsdb.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final String LOGS_PATTERN = "logs-*-*";
    private volatile boolean isLogsdbEnabled;

    LogsdbIndexModeSettingsProvider(final Settings settings) {
        this.isLogsdbEnabled = CLUSTER_LOGSDB_ENABLED.get(settings);
    }

    void updateClusterIndexModeLogsdbEnabled(boolean isLogsdbEnabled) {
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        final String indexName,
        final String dataStreamName,
        IndexMode templateIndexMode,
        final Metadata metadata,
        final Instant resolvedAt,
        final Settings settings,
        final List<CompressedXContent> combinedTemplateMappings
    ) {
        return getLogsdbModeSetting(dataStreamName, settings);
    }

    Settings getLogsdbModeSetting(final String dataStreamName, final Settings settings) {
        if (isLogsdbEnabled
            && dataStreamName != null
            && resolveIndexMode(settings.get(IndexSettings.MODE.getKey())) == null
            && matchesLogsPattern(dataStreamName)) {
            return Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build();
        }
        return Settings.EMPTY;
    }

    private static boolean matchesLogsPattern(final String name) {
        return Regex.simpleMatch(LOGS_PATTERN, name);
    }

    private IndexMode resolveIndexMode(final String mode) {
        return mode != null ? Enum.valueOf(IndexMode.class, mode.toUpperCase(Locale.ROOT)) : null;
    }
}
