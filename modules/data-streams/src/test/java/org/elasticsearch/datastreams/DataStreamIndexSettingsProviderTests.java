/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.datastreams.DataStreamIndexSettingsProvider.FORMATTER;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    public void testGetAdditionalIndexSettings() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = Settings.EMPTY;
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            metadata,
            now,
            settings
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT)));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            metadata,
            now,
            settings
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT)));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() {
        String dataStreamName = "logs-app1";
        TimeValue lookAheadTime = TimeValue.timeValueHours(2);

        Instant sixHoursAgo = Instant.now().minus(6, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
        Instant currentEnd = sixHoursAgo.plusMillis(lookAheadTime.getMillis());
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(new Tuple<>(sixHoursAgo, currentEnd))
        ).getMetadata();

        Instant now = sixHoursAgo.plus(6, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        var provider = new DataStreamIndexSettingsProvider();
        var result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.TIME_SERIES,
            metadata,
            now,
            settings
        );
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT)));
        assertThat(result.get(IndexSettings.TIME_SERIES_START_TIME.getKey()), equalTo(FORMATTER.format(currentEnd)));
        assertThat(
            result.get(IndexSettings.TIME_SERIES_END_TIME.getKey()),
            equalTo(FORMATTER.format(now.plusMillis(lookAheadTime.getMillis())))
        );
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreatedTimeSettingsMissing() {
        String dataStreamName = "logs-app1";
        Instant twoHoursAgo = Instant.now().minus(4, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                twoHoursAgo.toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        DataStream ds = mb.dataStream(dataStreamName);
        mb.put(
            new DataStream(
                ds.getName(),
                ds.getIndices(),
                ds.getGeneration(),
                ds.getMetadata(),
                ds.isHidden(),
                ds.isReplicated(),
                ds.isSystem(),
                ds.isAllowCustomRouting(),
                IndexMode.TIME_SERIES
            )
        );
        Metadata metadata = mb.build();

        Instant now = twoHoursAgo.plus(2, ChronoUnit.HOURS);
        Settings settings = Settings.EMPTY;
        var provider = new DataStreamIndexSettingsProvider();
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> provider.getAdditionalIndexSettings(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                dataStreamName,
                IndexMode.TIME_SERIES,
                metadata,
                now,
                settings
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "backing index [%s] in tsdb mode doesn't have the [index.time_series.end_time] index setting".formatted(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1, twoHoursAgo.toEpochMilli())
                )
            )
        );
    }

    public void testGetAdditionalIndexSettingsIndexModeNotSpecified() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Settings settings = Settings.EMPTY;
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            null,
            metadata,
            Instant.ofEpochMilli(1L),
            settings
        );
        assertThat(result.size(), equalTo(0));
    }

    public void testGetAdditionalIndexSettingsIndexModeStandardSpecified() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Settings settings = Settings.EMPTY;
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            IndexMode.STANDARD,
            metadata,
            Instant.ofEpochMilli(1L),
            settings
        );
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(IndexSettings.MODE.getKey()), equalTo(IndexMode.STANDARD.name().toLowerCase(Locale.ROOT)));
    }

}
