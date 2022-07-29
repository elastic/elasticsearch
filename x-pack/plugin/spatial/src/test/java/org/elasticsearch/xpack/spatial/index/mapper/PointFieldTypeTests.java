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
import java.util.List;
import java.util.Map;

public class PointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new PointFieldMapper.Builder("field", false).build(MapperBuilderContext.ROOT).fieldType();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        String wktPoint = "POINT (42.0 27.1)";
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [x, y] array format.
        Object sourceValue = List.of(42.0, 27.1);
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in "x, y" string format.
        sourceValue = "42.0,27.1";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single point
        sourceValue = "foo";
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [x, y] array format.
        sourceValue = List.of(List.of(42.0, 27.1), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [x, y] array format with a malformed entry
        sourceValue = List.of(List.of(42.0, 27.1), List.of("a", "b"), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

    }
}
