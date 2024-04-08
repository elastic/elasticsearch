/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new PointFieldMapper.Builder("field", false).build(MapperBuilderContext.root(false, false)).fieldType();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        String wktPoint = "POINT (42.0 27.1)";
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String otherWktPoint = "POINT (30.0 50.0)";
        byte[] wkbPoint = WellKnownBinary.toWKB(new Point(42.0, 27.1), ByteOrder.LITTLE_ENDIAN);
        byte[] otherWkbPoint = WellKnownBinary.toWKB(new Point(30.0, 50.0), ByteOrder.LITTLE_ENDIAN);

        // Test a single point in [x, y] array format.
        Object sourceValue = List.of(42.0, 27.1);
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        List<?> wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a single point in "x, y" string format.
        sourceValue = "42.0,27.1";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a malformed single point
        sourceValue = "foo";
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkb"));

        // Test a list of points in [x, y] array format.
        sourceValue = List.of(List.of(42.0, 27.1), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbPoint));
        assertThat(wkb.get(1), equalTo(otherWkbPoint));

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
