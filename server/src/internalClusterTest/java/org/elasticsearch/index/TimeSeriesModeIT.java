/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * @author weizijun.wzj
 * @date 2021/11/13
 */
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

        Exception e = expectThrows(
            IllegalStateException.class,
            () -> prepareCreate("test").setSettings(s).setMapping(mappings).get()
        );
        assertThat(
            e.getMessage(),
            equalTo("[_data_stream_timestamp] meta field has been disabled")
        );
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
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: time series index [_data_stream_timestamp] meta field format error"));
    }
}
