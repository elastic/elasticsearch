/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.legacygeo.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper.GeoShapeFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class LegacyGeoShapeFieldTypeTests extends FieldTypeTestCase {

    /**
     * Test for {@link LegacyGeoShapeFieldMapper.GeoShapeFieldType#setStrategy(SpatialStrategy)} that checks
     * that {@link LegacyGeoShapeFieldMapper.GeoShapeFieldType#pointsOnly()} gets set as a side effect when using SpatialStrategy.TERM
     */
    public void testSetStrategyName() {
        GeoShapeFieldType fieldType = new GeoShapeFieldType("field");
        assertFalse(fieldType.pointsOnly());
        fieldType.setStrategy(SpatialStrategy.RECURSIVE);
        assertFalse(fieldType.pointsOnly());
        fieldType.setStrategy(SpatialStrategy.TERM);
        assertTrue(fieldType.pointsOnly());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new LegacyGeoShapeFieldMapper.Builder("field", Version.CURRENT, false, true).build(
            MapperBuilderContext.ROOT
        ).fieldType();

        Map<String, Object> jsonLineString = org.elasticsearch.core.Map.of(
            "type",
            "LineString",
            "coordinates",
            Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0))
        );
        Map<String, Object> jsonPoint = org.elasticsearch.core.Map.of("type", "Point", "coordinates", Arrays.asList(14.0, 15.0));
        Map<String, Object> jsonMalformed = org.elasticsearch.core.Map.of("type", "LineString", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";
        String wktMalformed = "POINT foo";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in geojson format
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, jsonMalformed, null));
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, "POINT (a,b)", "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = Arrays.asList(jsonLineString, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes with one malformed in geojson format
        sourceValue = Arrays.asList(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in wkt format
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, wktMalformed, null));
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, wktMalformed, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = Arrays.asList(wktLineString, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes with one malformed in wkt format
        sourceValue = Arrays.asList(wktLineString, wktMalformed, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
