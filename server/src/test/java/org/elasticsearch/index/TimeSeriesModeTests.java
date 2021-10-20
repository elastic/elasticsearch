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
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

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
            equalTo("All fields that match routing_path must be keyword time_series_dimensions but [dim.o] was [object]")
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
                "All fields that match routing_path must be keyword time_series_dimensions but "
                    + "[dim.non_dim] was not a time_series_dimension"
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
            equalTo("All fields that match routing_path must be keyword time_series_dimensions but [dim.non_kwd] was [integer]")
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
}
