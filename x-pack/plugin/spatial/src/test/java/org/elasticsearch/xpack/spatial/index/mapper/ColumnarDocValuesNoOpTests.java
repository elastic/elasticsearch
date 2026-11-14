/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that {@code doc_values} is a no-op on the spatial mappers that use the plain doc values parameter when the index is
 * strict-columnar: doc values are always on, the parameter is dropped from the mapping, and {@code false} logs a deprecation warning.
 */
public class ColumnarDocValuesNoOpTests extends MapperServiceTestCase {

    private static final List<String> SPATIAL_TYPES = List.of("point", "shape", "geo_shape");

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    private static Settings settings(IndexMode mode) {
        return Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
    }

    private static String warning() {
        return "The [doc_values] parameter of field [field] is ignored in columnar index modes, because doc values are always enabled. "
            + "Remove the parameter.";
    }

    public void testDocValuesFalseIsIgnoredInColumnar() throws IOException {
        for (IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            for (String type : SPATIAL_TYPES) {
                MapperService mapperService = createMapperService(
                    settings(mode),
                    fieldMapping(b -> b.field("type", type).field("doc_values", false))
                );
                assertWarnings(warning());
                assertTrue(type, mapperService.fieldType("field").hasDocValues());
                assertThat(type, mapperService.documentMapper().mappingSource().string(), not(containsString("doc_values")));
            }
        }
    }

    public void testDocValuesTrueIsSilentlyIgnoredInColumnar() throws IOException {
        for (String type : SPATIAL_TYPES) {
            MapperService mapperService = createMapperService(
                settings(IndexMode.COLUMNAR),
                fieldMapping(b -> b.field("type", type).field("doc_values", true))
            );
            ensureNoWarnings();
            assertTrue(type, mapperService.fieldType("field").hasDocValues());
            assertThat(type, mapperService.documentMapper().mappingSource().string(), not(containsString("doc_values")));
        }
    }

    public void testDocValuesFalseStillHonouredOutsideColumnar() throws IOException {
        for (String type : SPATIAL_TYPES) {
            MapperService mapperService = createMapperService(
                settings(IndexMode.STANDARD),
                fieldMapping(b -> b.field("type", type).field("doc_values", false))
            );
            assertFalse(type, mapperService.fieldType("field").hasDocValues());
        }
    }

    public void testDocValuesFalseBeforeAllMappersGateIsNotIgnored() {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.DOC_VALUES_TOP_LEVEL_ATTRIBUTES,
            IndexVersionUtils.getPreviousVersion(IndexVersions.DOC_VALUES_NO_OP_IN_COLUMNAR_ALL_MAPPERS)
        );
        for (String type : SPATIAL_TYPES) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(
                    version,
                    settings(IndexMode.COLUMNAR),
                    fieldMapping(b -> b.field("type", type).field("doc_values", false))
                )
            );
            assertThat(type, e.getMessage(), containsString("cannot reconstruct _source from doc values"));
        }
    }
}
