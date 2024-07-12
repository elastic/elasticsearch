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
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.StringFieldScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesModeTests extends MapperServiceTestCase {

    public void testConfigureIndex() {
        Settings s = getSettings();
        assertSame(IndexMode.TIME_SERIES, IndexSettings.MODE.get(s));
    }

    public void testPartitioned() {
        Settings s = Settings.builder()
            .put(getSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2)
            .build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.routing_partition_size]"));
    }

    public void testSortField() {
        Settings s = Settings.builder().put(getSettings()).put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "a").build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.field]"));
    }

    public void testSortMode() {
        Settings s = Settings.builder().put(getSettings()).put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_last").build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.missing]"));
    }

    public void testSortOrder() {
        Settings s = Settings.builder().put(getSettings()).put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc").build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.order]"));
    }

    public void testWithoutRoutingPath() {
        Settings s = Settings.builder().put(IndexSettings.MODE.getKey(), "time_series").build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] requires a non-empty [index.routing_path]"));
    }

    public void testWithEmptyRoutingPath() {
        Settings s = getSettings("");
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", s);
        Exception e = expectThrows(IllegalArgumentException.class, () -> new IndexSettings(metadata, Settings.EMPTY));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] requires a non-empty [index.routing_path]"));
    }

    public void testWithoutStartTime() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", settings);

        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(indexSettings.getTimestampBounds().startTime(), CoreMatchers.equalTo(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999));
        assertThat(indexSettings.getTimestampBounds().endTime(), CoreMatchers.equalTo(DateUtils.MAX_MILLIS_BEFORE_9999));
    }

    public void testWithoutEndTime() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), "1970-01-01T00:00:00Z")
            .build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", settings);

        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(indexSettings.getTimestampBounds().startTime(), CoreMatchers.equalTo(0L));
        assertThat(indexSettings.getTimestampBounds().endTime(), CoreMatchers.equalTo(DateUtils.MAX_MILLIS_BEFORE_9999));
    }

    public void testSetDefaultTimeRangeValue() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999).toString())
            .put(TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999).toString())
            .build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", settings);
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(indexSettings.getTimestampBounds().startTime(), CoreMatchers.equalTo(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999));
        assertThat(indexSettings.getTimestampBounds().endTime(), CoreMatchers.equalTo(DateUtils.MAX_MILLIS_BEFORE_9999));
    }

    public void testRequiredRouting() {
        Settings s = getSettings();
        var mapperService = new TestMapperServiceBuilder().settings(s).applyDefaultMapping(false).build();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> withMapping(mapperService, topMapping(b -> b.startObject("_routing").field("required", true).endObject()))
        );
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testValidateAlias() {
        Settings s = getSettings();
        IndexSettings.MODE.get(s).validateAlias(null, null); // Doesn't throw exception
    }

    public void testValidateAliasWithIndexRouting() {
        Settings s = getSettings();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s).validateAlias("r", null));
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testValidateAliasWithSearchRouting() {
        Settings s = getSettings();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s).validateAlias(null, "r"));
        assertThat(e.getMessage(), equalTo("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]"));
    }

    public void testRoutingPathMatchesObject() throws IOException {
        Settings s = getSettings("dim.o*");
        createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            {
                b.startObject("o").startObject("properties");
                b.startObject("inner_dim").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.endObject().endObject();
            }
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
    }

    public void testRoutingPathEqualsObjectNameError() {
        Settings s = getSettings("dim.o");
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
                "All fields that match routing_path must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. [dim.o] was [object]."
            )
        );
    }

    public void testRoutingPathMatchesNonDimensionKeyword() {
        Settings s = getSettings(randomBoolean() ? "dim.non_dim" : "dim.*");
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("non_dim").field("type", "keyword").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. [dim.non_dim] was not a dimension."
            )
        );
    }

    public void testRoutingPathMatchesNonKeyword() throws IOException {
        Settings s = getSettings(randomBoolean() ? "dim.non_kwd" : "dim.*");
        createMapperService(s, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("non_kwd").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
    }

    public void testRoutingPathMatchesScriptedKeyword() {
        Settings s = getSettings(randomBoolean() ? "dim.kwd" : "dim.*");
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
                "All fields that match routing_path must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. [dim.kwd] has a [script] parameter."
            )
        );
    }

    public void testRoutingPathMatchesRuntimeKeyword() {
        Settings s = getSettings(randomBoolean() ? "dim.kwd" : "dim.*");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(s, runtimeMapping(b -> b.startObject("dim.kwd").field("type", "keyword").endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "All fields that match routing_path must be configured with [time_series_dimension: true] "
                    + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                    + "without the [script] parameter. [dim.kwd] was a runtime [keyword]."
            )
        );
    }

    public void testRoutingPathMatchesOnlyKeywordDimensions() throws IOException {
        Settings s = getSettings(randomBoolean() ? "dim.metric_type,dim.server,dim.species,dim.uuid" : "dim.*");
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
                public LeafFactory newFactory(
                    String fieldName,
                    Map<String, Object> params,
                    SearchLookup searchLookup,
                    OnScriptError onScriptError
                ) {
                    throw new UnsupportedOperationException("error should be thrown before getting here");
                }
            };
        }
        return super.compileScript(script, context);
    }

    private Settings getSettings() {
        return getSettings(randomAlphaOfLength(5), "2021-04-28T00:00:00Z", "2021-04-29T00:00:00Z");
    }

    private Settings getSettings(String routingPath) {
        return getSettings(routingPath, "2021-04-28T00:00:00Z", "2021-04-29T00:00:00Z");
    }

    private Settings getSettings(String routingPath, String startTime, String endTime) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .build();
    }
}
