/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ShapeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws Exception {
        MappedFieldType mapper = new ShapeFieldMapper.Builder("field", IndexVersion.current(), false, true).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();

        Map<String, Object> jsonLineString = Map.of("type", "LineString", "coordinates", List.of(List.of(42.0, 27.1), List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(14.3, 15.0));
        Map<String, Object> jsonMalformed = Map.of("type", "Point", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.3 15.0)";
        String wktMalformed = "POINT foo";
        byte[] wkbLine = WellKnownBinary.toWKB(
            WellKnownText.fromWKT(StandardValidator.NOOP, false, wktLineString),
            ByteOrder.LITTLE_ENDIAN
        );
        byte[] wkbPoint = WellKnownBinary.toWKB(WellKnownText.fromWKT(StandardValidator.NOOP, false, wktPoint), ByteOrder.LITTLE_ENDIAN);

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));
        List<?> wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbLine));

        // Test a malformed single shape in geojson format
        sourceValue = jsonMalformed;
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkb"));

        // Test a list of shapes in geojson format.
        sourceValue = List.of(jsonLineString, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbLine));
        assertThat(wkb.get(1), equalTo(wkbPoint));

        // Test a list of shapes including one malformed in geojson format
        sourceValue = List.of(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbLine));
        assertThat(wkb.get(1), equalTo(wkbPoint));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single malformed shape in wkt format
        sourceValue = wktMalformed;
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = List.of(wktLineString, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in wkt format
        sourceValue = List.of(wktLineString, wktMalformed, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

    }
}
