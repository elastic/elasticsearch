/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.datastreams.DataStreamIndexSettingsProvider.FORMATTER;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    public void testGetAdditionalIndexSettings() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            metadata,
            now.toEpochMilli(),
            settings
        );
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            metadata,
            now.toEpochMilli(),
            settings
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsNoTimeSeries() {
        Metadata metadata = Metadata.EMPTY_METADATA;
        String dataStreamName = "logs-app1";

        long now = Instant.now().toEpochMilli();
        Settings settings = randomBoolean() ? Settings.EMPTY : builder().put("index.mode", "standard").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            metadata,
            now,
            settings
        );
        assertThat(result, equalTo(Settings.EMPTY));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() {
        String dataStreamName = "logs-app1";
        TimeValue lookAheadTime = TimeValue.timeValueHours(2);

        Instant sixHoursAgo = Instant.now().minus(6, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        Instant currentEnd = sixHoursAgo.plusMillis(lookAheadTime.getMillis());
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(Tuple.tuple(dataStreamName, 1)),
            List.of(),
            sixHoursAgo.toEpochMilli(),
            builder().put(IndexSettings.TIME_SERIES_START_TIME.getKey(), FORMATTER.format(sixHoursAgo))
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), FORMATTER.format(currentEnd))
                .build(),
            1
        ).getMetadata();

        Instant now = sixHoursAgo.plus(6, ChronoUnit.HOURS);
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        var result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            metadata,
            now.toEpochMilli(),
            settings
        );
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(IndexSettings.TIME_SERIES_START_TIME.getKey()), equalTo(FORMATTER.format(currentEnd)));
        assertThat(
            result.get(IndexSettings.TIME_SERIES_END_TIME.getKey()),
            equalTo(FORMATTER.format(now.plusMillis(lookAheadTime.getMillis())))
        );
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreatedTimeSettingsMissing() {
        String dataStreamName = "logs-app1";
        Instant twoHoursAgo = Instant.now().minus(4, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(Tuple.tuple(dataStreamName, 1)),
            List.of(),
            twoHoursAgo.toEpochMilli(),
            builder().build(),
            1
        ).getMetadata();

        Instant now = twoHoursAgo.plus(2, ChronoUnit.HOURS);
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> provider.getAdditionalIndexSettings(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                dataStreamName,
                metadata,
                now.toEpochMilli(),
                settings
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "backing index [%s] in tsdb mode doesn't have the [index.time_series.start_time] index setting".formatted(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1, twoHoursAgo.toEpochMilli())
                )
            )
        );
    }

}
