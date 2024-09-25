/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    private static final Logger logger = LogManager.getLogger(LogsdbIndexModeSettingsProvider.class);

    public static final Setting<Boolean> CLUSTER_LOGSDB_ENABLED = Setting.boolSetting(
        "cluster.logsdb.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final String LOGS_PATTERN = "logs-*-*";
    private volatile boolean isLogsdbEnabled;

    public LogsdbIndexModeSettingsProvider(final Settings settings) {
        this.isLogsdbEnabled = CLUSTER_LOGSDB_ENABLED.get(settings);
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

        if (dataStreamName == null) {
            return Settings.EMPTY;
        }

        final IndexMode indexMode = resolveIndexMode(settings.get("index.mode"));
        if (indexMode != null) {
            return Settings.EMPTY;
        }

        if (usesLogsAtSettingsComponentTemplate(metadata, dataStreamName) && matchesLogsPattern(dataStreamName)) {
            return Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build();
        }

        return Settings.EMPTY;
    }

    private static boolean matchesLogsPattern(final String name) {
        return Regex.simpleMatch(LOGS_PATTERN, name);
    }

    private IndexMode resolveIndexMode(final String mode) {
        return mode == null
            ? null
            : Arrays.stream(IndexMode.values()).filter(indexMode -> Objects.equals(indexMode.getName(), mode)).findFirst().orElse(null);
    }

    private boolean usesLogsAtSettingsComponentTemplate(final Metadata metadata, final String name) {
        final String template = MetadataIndexTemplateService.findV2Template(metadata, name, false);
        if (template == null) {
            return false;
        }
        final ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(template);
        if (composableIndexTemplate == null) {
            return false;
        }
        return composableIndexTemplate.composedOf()
            .stream()
            .anyMatch(componentTemplate -> Objects.equals("logs@settings", componentTemplate));
    }

}
