/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_PATH;
import static org.elasticsearch.xpack.logsdb.LogsDBPlugin.CLUSTER_LOGSDB_ENABLED;

final class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
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
        return getLogsdbModeSetting(dataStreamName, templateIndexMode, settings);
    }

    Settings getLogsdbModeSetting(final String dataStreamName, final IndexMode templateIndexMode, final Settings settings) {
        if (isLogsdbEnabled && dataStreamName != null) {
            IndexMode mode = resolveIndexMode(settings.get(IndexSettings.MODE.getKey()));
            var builder = Settings.builder();
            if (mode == null && matchesLogsPattern(dataStreamName)) {
                builder.put("index.mode", IndexMode.LOGSDB.getName()).build();
                mode = IndexMode.LOGSDB;
            }
            if (mode == IndexMode.LOGSDB || templateIndexMode == IndexMode.LOGSDB) {
                if (settings.getAsBoolean(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), false)) {
                    List<String> sortFields = new ArrayList<>(settings.getAsList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey()));
                    sortFields.removeIf(s -> s.equals(DataStreamTimestampFieldMapper.DEFAULT_PATH));
                    if (sortFields.size() < 2) {
                        throw new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "data stream [%s] in logsdb mode and with [%s] index setting has only %d sort fields "
                                    + "(excluding timestamp), needs at least 2",
                                dataStreamName,
                                IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(),
                                sortFields.size()
                            )
                        );
                    }
                    if (settings.hasValue(IndexMetadata.INDEX_ROUTING_PATH.getKey())) {
                        List<String> routingPaths = settings.getAsList(IndexMetadata.INDEX_ROUTING_PATH.getKey());
                        if (routingPaths.equals(sortFields) == false) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "data stream [%s] in logsdb mode and with [%s] index setting has mismatching sort "
                                        + "and routing fields, [index.routing_path:%s], [index.sort.fields:%s]",
                                    dataStreamName,
                                    IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(),
                                    routingPaths,
                                    sortFields
                                )
                            );
                        }
                    } else {
                        builder.putList(INDEX_ROUTING_PATH.getKey(), sortFields);
                    }
                }
            }
            if (builder.keys().isEmpty() == false) {
                return builder.build();
            }

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
