/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeload;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;

import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

class WriteLoadForecastIndexSettingProvider implements IndexSettingProvider {
    private final BooleanSupplier hasValidLicense;

    WriteLoadForecastIndexSettingProvider(BooleanSupplier hasValidLicense) {
        this.hasValidLicense = hasValidLicense;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean timeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        if (dataStreamName != null && metadata.dataStreams().get(dataStreamName) != null && hasValidLicense.getAsBoolean()) {
            var settingsBuilder = Settings.builder().put(IndexSettings.FORECAST_WRITE_LOAD_SETTING.getKey(), true);
            if (allSettings.hasValue(IndexSettings.DEFAULT_WRITE_LOAD_SETTING.getKey())) {
                // TODO: warn when the setting exists and the license is invalid?
                settingsBuilder.put(
                    IndexSettings.DEFAULT_INTERNAL_WRITE_LOAD_SETTING.getKey(),
                    IndexSettings.DEFAULT_WRITE_LOAD_SETTING.get(allSettings)
                );
            }
            return settingsBuilder.build();
        } else {
            return Settings.EMPTY;
        }
    }
}
