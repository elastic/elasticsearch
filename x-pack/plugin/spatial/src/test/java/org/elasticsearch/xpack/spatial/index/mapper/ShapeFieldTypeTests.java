/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ShapeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new ShapeFieldMapper.Builder("field", false, true).build(MapperBuilderContext.ROOT).fieldType();

        Map<String, Object> jsonLineString = org.elasticsearch.core.Map.of(
            "type",
            "LineString",
            "coordinates",
            Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0))
        );
        Map<String, Object> jsonPoint = org.elasticsearch.core.Map.of("type", "Point", "coordinates", Arrays.asList(14.3, 15.0));
        Map<String, Object> jsonMalformed = org.elasticsearch.core.Map.of("type", "Point", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.3 15.0)";
        String wktMalformed = "POINT foo";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in geojson format
        sourceValue = jsonMalformed;
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = Arrays.asList(jsonLineString, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in geojson format
        sourceValue = Arrays.asList(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single malformed shape in wkt format
        sourceValue = wktMalformed;
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = Arrays.asList(wktLineString, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in wkt format
        sourceValue = Arrays.asList(wktLineString, wktMalformed, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
