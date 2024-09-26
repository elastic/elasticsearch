/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * An index setting provider that overwrites the source mode from synthetic to stored if synthetic source isn't allowed to be used.
 */
public class SyntheticSourceIndexSettingsProvider implements IndexSettingProvider {

    private static final Logger LOGGER = LogManager.getLogger(SyntheticSourceIndexSettingsProvider.class);

    private final SyntheticSourceLicenseService syntheticSourceLicenseService;

    public SyntheticSourceIndexSettingsProvider(SyntheticSourceLicenseService syntheticSourceLicenseService) {
        this.syntheticSourceLicenseService = syntheticSourceLicenseService;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean isTimeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        if (newIndexHasSyntheticSourceUsage(indexTemplateAndCreateRequestSettings)
            && syntheticSourceLicenseService.fallbackToStoredSource()) {
            LOGGER.debug("creation of index [{}] with synthetic source without it being allowed", indexName);
            // TODO: handle falling back to stored source
        }
        return Settings.EMPTY;
    }

    boolean newIndexHasSyntheticSourceUsage(Settings indexTemplateAndCreateRequestSettings) {
        // TODO: build tmp MapperService and check whether SourceFieldMapper#isSynthetic() to determine synthetic source usage.
        // Not using IndexSettings.MODE.get() to avoid validation that may fail at this point.
        var rawIndexMode = indexTemplateAndCreateRequestSettings.get(IndexSettings.MODE.getKey());
        IndexMode indexMode = rawIndexMode != null ? Enum.valueOf(IndexMode.class, rawIndexMode.toUpperCase(Locale.ROOT)) : null;
        return indexMode != null && indexMode.isSyntheticSourceEnabled();
    }
}
