/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    public void testGetAdditionalIndexSettings() {
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        TimeValue lookAheadTime = TimeValue.timeValueHours(2); // default
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            now.toEpochMilli(),
            settings
        );
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() {
        String dataStreamName = "logs-app1";

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            now.toEpochMilli(),
            settings
        );
        assertThat(result.size(), equalTo(2));
        assertThat(IndexSettings.TIME_SERIES_START_TIME.get(result), equalTo(now.minusMillis(lookAheadTime.getMillis())));
        assertThat(IndexSettings.TIME_SERIES_END_TIME.get(result), equalTo(now.plusMillis(lookAheadTime.getMillis())));
    }

    public void testGetAdditionalIndexSettingsNoTimeSeries() {
        String dataStreamName = "logs-app1";

        long now = Instant.now().toEpochMilli();
        Settings settings = randomBoolean() ? Settings.EMPTY : builder().put("index.mode", "standard").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            true,
            now,
            settings
        );
        assertThat(result, equalTo(Settings.EMPTY));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() {
        String dataStreamName = "logs-app1";

        long now = Instant.now().toEpochMilli();
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            false,
            now,
            settings
        );
        assertThat(result, equalTo(Settings.EMPTY));
    }

}
