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
import java.util.List;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamIndexSettingsProviderTests extends ESTestCase {

    public void testGetAdditionalIndexSettings() {
        String dataStreamName = "logs-app1";
        Metadata metadata = Metadata.EMPTY_METADATA;

        long now = Instant.now().toEpochMilli();
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            now,
            settings,
            metadata
        );
        assertThat(result.getAsLong(IndexSettings.TIME_SERIES_START_TIME.getKey(), -1L), equalTo(1L));
        assertThat(result.getAsLong(IndexSettings.TIME_SERIES_END_TIME.getKey(), -1L), equalTo(now + TimeValue.timeValueHours(2).millis()));
    }

    public void testGetAdditionalIndexSettingsLookAheadTime() {
        String dataStreamName = "logs-app1";
        Metadata metadata = Metadata.EMPTY_METADATA;

        long now = Instant.now().toEpochMilli();
        TimeValue lookAheadTime = TimeValue.timeValueMinutes(30);
        Settings settings = builder().put("index.mode", "time_series").put("index.look_ahead_time", lookAheadTime.getStringRep()).build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            now,
            settings,
            metadata
        );
        assertThat(result.size(), equalTo(2));
        assertThat(result.getAsLong(IndexSettings.TIME_SERIES_START_TIME.getKey(), -1L), equalTo(1L));
        assertThat(result.getAsLong(IndexSettings.TIME_SERIES_END_TIME.getKey(), -1L), equalTo(now + lookAheadTime.millis()));
    }

    public void testGetAdditionalIndexSettingsNoTimeSeries() {
        String dataStreamName = "logs-app1";
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(Tuple.tuple(dataStreamName, 1)), List.of())
            .getMetadata();

        long now = Instant.now().toEpochMilli();
        Settings settings = randomBoolean() ? Settings.EMPTY : builder().put("index.mode", "standard").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            now,
            settings,
            metadata
        );
        assertThat(result, equalTo(Settings.EMPTY));
    }

    public void testGetAdditionalIndexSettingsDataStreamAlreadyCreated() {
        String dataStreamName = "logs-app1";
        Metadata metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(Tuple.tuple(dataStreamName, 1)), List.of())
            .getMetadata();

        long now = Instant.now().toEpochMilli();
        Settings settings = builder().put("index.mode", "time_series").build();
        var provider = new DataStreamIndexSettingsProvider();
        Settings result = provider.getAdditionalIndexSettings(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            dataStreamName,
            now,
            settings,
            metadata
        );
        assertThat(result, equalTo(Settings.EMPTY));
    }

}
