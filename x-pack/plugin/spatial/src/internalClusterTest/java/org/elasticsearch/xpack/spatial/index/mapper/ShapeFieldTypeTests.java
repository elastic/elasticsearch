/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ShapeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new ShapeFieldMapper.Builder("field").build(context).fieldType();

        Map<String, Object> jsonLineString = org.elasticsearch.common.collect.Map.of("type", "LineString", "coordinates",
            Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0)));
        Map<String, Object> jsonPoint = org.elasticsearch.common.collect.Map.of(
            "type", "Point",
            "coordinates", Arrays.asList(14.3, 15.0));
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.3 15.0)";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = Arrays.asList(jsonLineString, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = Arrays.asList(wktLineString, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
