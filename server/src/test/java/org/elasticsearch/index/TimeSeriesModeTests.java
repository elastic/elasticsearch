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
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.StringFieldScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesModeTests extends MapperServiceTestCase {

    public void testConfigureIndex() {
        Settings s = getSettings();
        assertSame(IndexMode.TIME_SERIES, IndexSettings.MODE.get(s));
    }

    public void testPartitioned() {
        Settings s = Settings.builder().put(getSettings()).put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.routing_partition_size]"));
    }

    public void testSortField() {
        Settings s = Settings.builder().put(getSettings()).put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "a").build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.field]"));
    }

    public void testSortMode() {
        Settings s = Settings.builder().put(getSettings())
            .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_last")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.missing]"));
    }

    public void testSortOrder() {
        Settings s = Settings.builder().put(getSettings())
            .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.order]"));
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

    private Settings getSettings() {
        return getSettings(randomAlphaOfLength(5), 1, DateUtils.MAX_MILLIS_BEFORE_9999 - 1);
    }

    private Settings getSettings(String routingPath) {
        return getSettings(routingPath, 1, DateUtils.MAX_MILLIS_BEFORE_9999 - 1);
    }

    private Settings getSettings(String routingPath, long startTime, long endTime) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .build();
    }
}
