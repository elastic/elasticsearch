/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class ColumnarMappingRuntimeFieldsValidationTests extends MapperServiceTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("columnar index modes require snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    public void testColumnarRejectsSingleRootRuntimeField() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(
                columnarSettings(IndexMode.COLUMNAR),
                runtimeMapping(b -> b.startObject("runtime_keyword").field("type", "keyword").endObject())
            )
        );
        assertRuntimeFieldRejected(e, IndexMode.COLUMNAR);
    }

    public void testColumnarRejectsMultipleRootRuntimeFields() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(columnarSettings(IndexMode.COLUMNAR), runtimeMapping(b -> {
                b.startObject("runtime_keyword").field("type", "keyword").endObject();
                b.startObject("runtime_long").field("type", "long").endObject();
            }))
        );
        assertRuntimeFieldRejected(e, IndexMode.COLUMNAR);
    }

    public void testColumnarAcceptsMappingWithoutRuntimeFields() throws IOException {
        createMapperService(columnarSettings(IndexMode.COLUMNAR), mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("count").field("type", "long").endObject();
        }));
    }

    public void testLogsdbColumnarRejectsSingleRootRuntimeField() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(
                columnarSettings(IndexMode.LOGSDB_COLUMNAR),
                runtimeMapping(b -> b.startObject("runtime_keyword").field("type", "keyword").endObject())
            )
        );
        assertRuntimeFieldRejected(e, IndexMode.LOGSDB_COLUMNAR);
    }

    public void testLogsdbColumnarRejectsMultipleRootRuntimeFields() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(columnarSettings(IndexMode.LOGSDB_COLUMNAR), runtimeMapping(b -> {
                b.startObject("runtime_keyword").field("type", "keyword").endObject();
                b.startObject("runtime_long").field("type", "long").endObject();
            }))
        );
        assertRuntimeFieldRejected(e, IndexMode.LOGSDB_COLUMNAR);
    }

    public void testLogsdbColumnarAcceptsMappingWithoutRuntimeFields() throws IOException {
        createMapperService(columnarSettings(IndexMode.LOGSDB_COLUMNAR), mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("count").field("type", "long").endObject();
        }));
    }

    private static Settings columnarSettings(IndexMode mode) {
        return Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
    }

    private static void assertRuntimeFieldRejected(IllegalArgumentException e, IndexMode mode) {
        assertThat(e.getMessage(), containsString("mapping-level runtime fields are not allowed in index using [" + mode + "] index mode"));
    }
}
