/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexSettingProvider;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

public class DataStreamIndexSettingsProvider implements IndexSettingProvider {

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        long resolvedAt,
        Settings templateAndRequestSettings,
        Metadata metadata
    ) {
        if (dataStreamName != null && metadata.dataStreams().containsKey(dataStreamName) == false) {
            IndexMode indexMode = Optional.ofNullable(templateAndRequestSettings.get(IndexSettings.MODE.getKey()))
                .map(value -> IndexMode.valueOf(value.toUpperCase(Locale.ROOT)))
                .orElse(IndexMode.STANDARD);
            TimeValue lookAheadTime = templateAndRequestSettings.getAsTime(
                IndexSettings.LOOK_AHEAD_TIME.getKey(),
                IndexSettings.LOOK_AHEAD_TIME.getDefault(templateAndRequestSettings)
            );
            if (indexMode == IndexMode.TIME_SERIES) {
                // 0 is the default and this is a required setting and b/c of that another value should be used than 0
                Instant start = Instant.ofEpochMilli(1);
                Instant end = Instant.ofEpochMilli(resolvedAt).plusMillis(lookAheadTime.getMillis());

                Settings.Builder builder = Settings.builder();
                builder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start.toEpochMilli());
                builder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end.toEpochMilli());
                return builder.build();
            }
        }

        return Settings.EMPTY;
    }

}
