/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesModeIT extends ESIntegTestCase {
    public void testDisabledTimeStampMapper() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        XContentBuilder mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject(DataStreamTimestampFieldMapper.NAME)
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject();

        Exception e = expectThrows(IllegalStateException.class, () -> prepareCreate("test").setSettings(s).setMapping(mappings).get());
        assertThat(e.getMessage(), equalTo("[_data_stream_timestamp] meta field has been disabled"));
    }

    public void testBadTimeStampMapper() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        XContentBuilder mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .field(DataStreamTimestampFieldMapper.NAME, "enabled")
            .endObject()
            .endObject();

        Exception e = expectThrows(MapperParsingException.class, () -> prepareCreate("test").setSettings(s).setMapping(mappings).get());
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: [_parent] must be an object containing [type]"));
    }

    public void testBadTimestamp() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        String type = randomFrom("keyword", "integer", "long", "double", "text");
        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", type);
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();
        Exception e = expectThrows(IllegalArgumentException.class, () -> prepareCreate("test").setSettings(s).setMapping(mappings).get());
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [" + type + "], but [date,date_nanos] is expected")
        );
    }

    public void testAddsTimestamp() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).get();
        ensureGreen(index);

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        assertThat(getMappingsResponse.getMappings().size(), equalTo(1));

        XContentBuilder expect = XContentFactory.jsonBuilder();
        expect.startObject();
        {
            expect.startObject("_doc");
            {
                expect.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    expect.field("enabled", true);
                }
                expect.endObject();
                expect.startObject("properties");
                {
                    expect.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        expect.field("type", "date");
                    }
                    expect.endObject();
                }
                expect.endObject();
            }
            expect.endObject();
        }
        expect.endObject();
        assertThat(getMappingsResponse.getMappings().get(index).source().string(), equalTo(Strings.toString(expect)));
    }

    public void testTimestampMillis() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", "date");
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        XContentBuilder expect = XContentFactory.jsonBuilder();
        expect.startObject();
        {
            expect.startObject("_doc");
            {
                expect.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    expect.field("enabled", true);
                }
                expect.endObject();
                expect.startObject("properties");
                {
                    expect.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        expect.field("type", "date");
                    }
                    expect.endObject();
                }
                expect.endObject();
            }
            expect.endObject();
        }
        expect.endObject();
        assertThat(getMappingsResponse.getMappings().get(index).source().string(), equalTo(Strings.toString(expect)));
    }

    public void testTimestampNanos() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", "date_nanos");
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        XContentBuilder expect = XContentFactory.jsonBuilder();
        expect.startObject();
        {
            expect.startObject("_doc");
            {
                expect.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    expect.field("enabled", true);
                }
                expect.endObject();
                expect.startObject("properties");
                {
                    expect.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        expect.field("type", "date_nanos");
                    }
                    expect.endObject();
                }
                expect.endObject();
            }
            expect.endObject();
        }
        expect.endObject();
        assertThat(getMappingsResponse.getMappings().get(index).source().string(), equalTo(Strings.toString(expect)));
    }

    public void testWithoutTimestamp() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", "date");
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> index(index, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject())
        );
        assertThat(e.getRootCause().getMessage(), containsString("data stream timestamp field [@timestamp] is missing"));
    }

    public void testEnableTimestampRange() throws IOException {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);

        Settings s = Settings.builder()
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", randomBoolean() ? "date" : "date_nanos");
                    }
                    mappings.endObject();
                    mappings.startObject("foo");
                    {
                        mappings.field("type", "keyword");
                        mappings.field("time_series_dimension", true);
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        IndexResponse indexResponse = index(
            index,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar")
                .field("@timestamp", randomLongBetween(startTime, endTime))
                .endObject()
        );
        assertEquals(indexResponse.getResult(), Result.CREATED);
    }

    public void testBadStartTime() throws IOException {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);

        Settings s = Settings.builder()
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", randomBoolean() ? "date" : "date_nanos");
                    }
                    mappings.endObject();
                    mappings.startObject("foo");
                    {
                        mappings.field("type", "keyword");
                        mappings.field("time_series_dimension", true);
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> index(
                index,
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .field("@timestamp", Math.max(startTime - randomLongBetween(1, 3), 0))
                    .endObject()
            )
        );
        assertThat(e.getRootCause().getMessage(), containsString("must be larger than"));
    }

    public void testBadEndTime() throws IOException {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);

        Settings s = Settings.builder()
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", randomBoolean() ? "date" : "date_nanos");
                    }
                    mappings.endObject();
                    mappings.startObject("foo");
                    {
                        mappings.field("type", "keyword");
                        mappings.field("time_series_dimension", true);
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> index(
                index,
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .field("@timestamp", endTime + randomLongBetween(0, 3))
                    .endObject()
            )
        );
        assertThat(e.getRootCause().getMessage(), containsString("must be smaller than"));
    }

    public void testEnabledTimeStampMapper() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        XContentBuilder mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject(DataStreamTimestampFieldMapper.NAME);
        if (randomBoolean()) {
            mappings.field("enabled", true);
        } else {
            mappings.field("enabled", "true");
        }
        mappings.endObject().endObject().endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        XContentBuilder expect = XContentFactory.jsonBuilder();
        expect.startObject();
        {
            expect.startObject("_doc");
            {
                expect.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    expect.field("enabled", true);
                }
                expect.endObject();
                expect.startObject("properties");
                {
                    expect.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        expect.field("type", "date");
                    }
                    expect.endObject();
                }
                expect.endObject();
            }
            expect.endObject();
        }
        expect.endObject();
        assertThat(getMappingsResponse.getMappings().get(index).source().string(), equalTo(Strings.toString(expect)));
    }

    public void testAddTimeStampMeta() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startObject("_doc");
            {
                mappings.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    mappings.field("enabled", true);
                }
                mappings.endObject();
                mappings.startObject("properties");
                {
                    mappings.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        mappings.field("type", "date");
                        mappings.startObject("meta");
                        {
                            mappings.field("field_meta", "time_series");
                        }
                        mappings.endObject();
                    }
                    mappings.endObject();
                    mappings.startObject("foo");
                    {
                        mappings.field("type", "keyword");
                        mappings.field("time_series_dimension", true);
                    }
                    mappings.endObject();
                }
                mappings.endObject();
            }
            mappings.endObject();
        }
        mappings.endObject();

        String index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        prepareCreate(index).setSettings(s).setMapping(mappings).get();
        ensureGreen(index);

        IndexResponse indexResponse = index(
            index,
            XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar")
                .field("@timestamp", System.currentTimeMillis())
                .field("new_field", "value")
                .endObject()
        );
        assertEquals(indexResponse.getResult(), Result.CREATED);

        XContentBuilder expect = XContentFactory.jsonBuilder();
        expect.startObject();
        {
            expect.startObject("_doc");
            {
                expect.startObject(DataStreamTimestampFieldMapper.NAME);
                {
                    expect.field("enabled", true);
                }
                expect.endObject();
                expect.startObject("properties");
                {
                    expect.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    {
                        expect.field("type", "date");
                        expect.startObject("meta");
                        {
                            expect.field("field_meta", "time_series");
                        }
                        expect.endObject();
                    }
                    expect.endObject();
                    expect.startObject("foo");
                    {
                        expect.field("type", "keyword");
                        expect.field("time_series_dimension", true);
                    }
                    expect.endObject();
                    expect.startObject("new_field");
                    {
                        expect.field("type", "text");
                        expect.startObject("fields");
                        {
                            expect.startObject("keyword");
                            {
                                expect.field("type", "keyword");
                                expect.field("ignore_above", 256);
                            }
                            expect.endObject();
                        }
                        expect.endObject();
                    }
                    expect.endObject();
                }
                expect.endObject();
            }
            expect.endObject();
        }
        expect.endObject();
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        assertThat(getMappingsResponse.getMappings().get(index).source().string(), equalTo(Strings.toString(expect)));
    }

}
