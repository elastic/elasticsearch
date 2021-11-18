/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.StringFieldScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TimeSeriesModeTests extends MapperServiceTestCase {

    public void testConfigureIndex() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        assertSame(IndexMode.TIME_SERIES, IndexSettings.MODE.get(s));
    }

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
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> {})).documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.MILLISECONDS));

        Mapper timestampField = mapper.mappers().getMapper(DataStreamTimestampFieldMapper.NAME);
        assertThat(timestampField, instanceOf(DataStreamTimestampFieldMapper.class));
        assertTrue(((DataStreamTimestampFieldMapper) timestampField).isEnabled());
    }

    public void testTimestampMillis() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType("@timestamp");
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.MILLISECONDS));

        Mapper timestampField = mapper.mappers().getMapper(DataStreamTimestampFieldMapper.NAME);
        assertThat(timestampField, instanceOf(DataStreamTimestampFieldMapper.class));
        assertTrue(((DataStreamTimestampFieldMapper) timestampField).isEnabled());
    }

    public void testTimestampNanos() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date_nanos").endObject()))
            .documentMapper();
        MappedFieldType timestamp = mapper.mappers().getFieldType("@timestamp");
        assertThat(timestamp, instanceOf(DateFieldType.class));
        assertThat(((DateFieldType) timestamp).resolution(), equalTo(DateFieldMapper.Resolution.NANOSECONDS));

        Mapper timestampField = mapper.mappers().getMapper(DataStreamTimestampFieldMapper.NAME);
        assertThat(timestampField, instanceOf(DataStreamTimestampFieldMapper.class));
        assertTrue(((DataStreamTimestampFieldMapper) timestampField).isEnabled());
    }

    public void testBadTimestamp() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        String type = randomFrom("keyword", "integer", "long", "double", "text");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", type).endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [" + type + "], but [date,date_nanos] is expected")
        );
    }

    public void testWithoutTimestamp() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();

        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(
                new SourceToParse("1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()), XContentType.JSON)
            )
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
        DocumentMapper mapper = createMapperService(
            s,
            mapping(b -> b.startObject("@timestamp").field("type", randomBoolean() ? "date" : "date_nanos").endObject())
        ).documentMapper();
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("@timestamp", randomLongBetween(startTime, endTime)).endObject()
                ),
                XContentType.JSON
            )
        );
        // Look, mah, no failure.
        assertNotNull(doc.rootDoc().getNumericValue("@timestamp"));
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

        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("@timestamp", Math.max(startTime - randomLongBetween(1, 3), 0))
                            .endObject()
                    ),
                    XContentType.JSON
                )
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

        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("@timestamp", endTime + randomLongBetween(0, 3)).endObject()
                    ),
                    XContentType.JSON
                )
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

        DocumentMapper mapper = createMapperService(s, mappings).documentMapper();
        Mapper timestampField = mapper.mappers().getMapper(DataStreamTimestampFieldMapper.NAME);
        assertThat(timestampField, instanceOf(DataStreamTimestampFieldMapper.class));
        assertTrue(((DataStreamTimestampFieldMapper) timestampField).isEnabled());
    }

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

        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(s, mappings).documentMapper());
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse mapping: time series index [_data_stream_timestamp] meta field must be enabled")
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

        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(s, mappings).documentMapper());
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: time series index [_data_stream_timestamp] meta field format error"));
    }

    public void testWithoutRoutingPath() {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] requires [index.routing_path]"));
    }

    public void testRequiredRouting() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(s, topMapping(b -> b.startObject("_routing").field("required", true).endObject()))
        );
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testValidateAlias() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        IndexSettings.MODE.get(s).validateAlias(null, null); // Doesn't throw exception
    }

    public void testValidateAliasWithIndexRouting() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s).validateAlias("r", null));
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testValidateAliasWithSearchRouting() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s).validateAlias(null, "r"));
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testRoutingPathMatchesObject() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.o" : "dim.*")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            {
                b.startObject("o").startObject("properties");
                b.startObject("inner_dim").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.endObject().endObject();
            }
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "and without the [script] parameter. [dim.o] was [object]."
            )
        );
    }

    public void testRoutingPathMatchesNonDimensionKeyword() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.non_dim" : "dim.*")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("non_dim").field("type", "keyword").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "and without the [script] parameter. [dim.non_dim] was not [time_series_dimension: true]."
            )
        );
    }

    public void testRoutingPathMatchesNonKeyword() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.non_kwd" : "dim.*")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("non_kwd").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "and without the [script] parameter. [dim.non_kwd] was [integer]."
            )
        );
    }

    public void testRoutingPathMatchesScriptedKeyword() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.kwd" : "dim.*")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(s, mapping(b -> {
            b.startObject("dim.kwd");
            b.field("type", "keyword");
            b.field("time_series_dimension", true);
            b.startObject("script").field("lang", "mock").field("source", "mock").endObject();
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "and without the [script] parameter. [dim.kwd] has a [script] parameter."
            )
        );
    }

    public void testRoutingPathMatchesRuntimeKeyword() {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.kwd" : "dim.*")
            .build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(s, runtimeMapping(b -> b.startObject("dim.kwd").field("type", "keyword").endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                    + "and without the [script] parameter. [dim.kwd] was a runtime [keyword]."
            )
        );
    }

    public void testRoutingPathMatchesOnlyKeywordDimensions() throws IOException {
        Settings s = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), randomBoolean() ? "dim.metric_type,dim.server,dim.species,dim.uuid" : "dim.*")
            .build();
        createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("metric_type").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("server").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("species").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("uuid").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        })); // doesn't throw
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context.equals(StringFieldScript.CONTEXT) && script.getLang().equals("mock")) {
            return (T) new StringFieldScript.Factory() {
                @Override
                public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup) {
                    throw new UnsupportedOperationException("error should be thrown before getting here");
                }
            };
        }
        return super.compileScript(script, context);
    }
}
