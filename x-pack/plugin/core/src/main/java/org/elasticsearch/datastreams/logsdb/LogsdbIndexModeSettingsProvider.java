/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.datastreams.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    private static final Logger logger = LogManager.getLogger(LogsdbIndexModeSettingsProvider.class);

    public static final Setting<Boolean> CLUSTER_LOGSDB_ENABLED = Setting.boolSetting(
        "cluster.logsdb.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private boolean isLogsdbEnabled;

    public LogsdbIndexModeSettingsProvider(boolean isLogsdbEnabled) {
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    public void updateClusterIndexModeLogsdbEnabled(boolean isLogsdbEnabled) {
        logger.debug("LogsDB " + (isLogsdbEnabled ? "enabled" : "disabled"));
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        final String indexName,
        final String dataStreamName,
        boolean isTimeSeries,
        final Metadata metadata,
        final Instant resolvedAt,
        final Settings settings,
        final List<CompressedXContent> combinedTemplateMappings
    ) {
        if (isLogsdbEnabled == false) {
            return Settings.EMPTY;
        }

        final IndexMode indexMode = resolveIndexMode(settings.get("index.mode"));
        if (IndexMode.STANDARD.equals(indexMode) && isLogsDataStreamOrIndexName(indexName, dataStreamName)) {
            return Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build();
        }

        return Settings.EMPTY;
    }

    private static boolean isLogsDataStreamOrIndexName(final String indexName, final String dataStreamName) {
        return Regex.simpleMatch("logs-[^-]+-[^-]+", indexName) || Regex.simpleMatch("logs-[^-]+-[^-]+", dataStreamName);
    }

    private IndexMode resolveIndexMode(final String mode) {
        if (mode == null) {
            return IndexMode.STANDARD;
        }
        return Arrays.stream(IndexMode.values())
            .filter(indexMode -> indexMode.getName().equals(mode))
            .findFirst()
            .orElse(IndexMode.STANDARD);
    }

}
