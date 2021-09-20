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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TimeSeriesModeTests extends MapperServiceTestCase {
    public void testPartitioned() {
        Settings s = Settings.builder()
            .put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.routing_partition_size]"));
    }

    public void testSortField() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "a")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.field]"));
    }

    public void testSortMode() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_last")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.missing]"));
    }

    public void testSortOrder() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.order]"));
    }

    public void testAddsTimestamp() throws IOException {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> {})).documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType("@timestamp");
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.MILLISECONDS));
    }

    public void testTimestampMillis() throws IOException {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType("@timestamp");
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.MILLISECONDS));
    }

    public void testTimestampNanos() throws IOException {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date_nanos").endObject()))
            .documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType("@timestamp");
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.NANOSECONDS));
    }

    public void testBadTimestamp() throws IOException {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                s,
                mapping(b -> b.startObject("@timestamp").field("type", randomFrom("keyword", "int", "long", "double", "text")).endObject())
            )
        );
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: @timestamp must be [date] or [date_nanos]"));
    }
}
