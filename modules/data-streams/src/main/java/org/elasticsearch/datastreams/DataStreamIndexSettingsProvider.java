/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class DataStreamIndexSettingsProvider implements IndexSettingProvider {

    static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        IndexMode templateIndexMode,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings
    ) {
        if (dataStreamName != null) {
            DataStream dataStream = metadata.dataStreams().get(dataStreamName);
            // First backing index is created and then data stream is rolled over (in a single cluster state update).
            // So at this point we can't check index_mode==time_series,
            // so checking that index_mode==null|standard and templateIndexMode == TIME_SERIES
            boolean migrating = dataStream != null
                && (dataStream.getIndexMode() == null || dataStream.getIndexMode() == IndexMode.STANDARD)
                && templateIndexMode == IndexMode.TIME_SERIES;
            IndexMode indexMode;
            if (migrating) {
                indexMode = IndexMode.TIME_SERIES;
            } else if (dataStream != null) {
                indexMode = dataStream.getIndexMode();
            } else {
                indexMode = templateIndexMode;
            }
            if (indexMode != null) {
                Settings.Builder builder = Settings.builder();
                builder.put(IndexSettings.MODE.getKey(), indexMode);

                if (indexMode == IndexMode.TIME_SERIES) {
                    TimeValue lookAheadTime = IndexSettings.LOOK_AHEAD_TIME.get(allSettings);
                    final Instant start;
                    final Instant end;
                    if (dataStream == null || migrating) {
                        start = resolvedAt.minusMillis(lookAheadTime.getMillis()).truncatedTo(ChronoUnit.SECONDS);
                        end = resolvedAt.plusMillis(lookAheadTime.getMillis()).truncatedTo(ChronoUnit.SECONDS);
                    } else {
                        IndexMetadata currentLatestBackingIndex = metadata.index(dataStream.getWriteIndex());
                        if (currentLatestBackingIndex.getSettings().hasValue(IndexSettings.TIME_SERIES_END_TIME.getKey()) == false) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "backing index [%s] in tsdb mode doesn't have the [%s] index setting",
                                    currentLatestBackingIndex.getIndex().getName(),
                                    IndexSettings.TIME_SERIES_END_TIME.getKey()
                                )
                            );
                        }
                        start = IndexSettings.TIME_SERIES_END_TIME.get(currentLatestBackingIndex.getSettings());
                        if (start.isAfter(resolvedAt)) {
                            end = start.plusMillis(lookAheadTime.getMillis()).truncatedTo(ChronoUnit.SECONDS);
                        } else {
                            end = resolvedAt.plusMillis(lookAheadTime.getMillis()).truncatedTo(ChronoUnit.SECONDS);
                        }
                    }
                    assert start.isBefore(end) : "data stream backing index's start time is not before end time";
                    builder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), FORMATTER.format(start));
                    builder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), FORMATTER.format(end));
                }
                return builder.build();
            }
        }

        return Settings.EMPTY;
    }

}
