/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.legacygeo.mapper;

import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper.GeoShapeFieldType;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.List;
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
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
        MappedFieldType mapper = new LegacyGeoShapeFieldMapper.Builder("field", version, false, true).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();

        Map<String, Object> jsonLineString = Map.of("type", "LineString", "coordinates", List.of(List.of(42.0, 27.1), List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(14.0, 15.0));
        Map<String, Object> jsonMalformed = Map.of("type", "LineString", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";
        String wktMalformed = "POINT foo";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in geojson format
        assertEquals(List.of(), fetchSourceValue(mapper, jsonMalformed, null));
        assertEquals(List.of(), fetchSourceValue(mapper, "POINT (a,b)", "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = List.of(jsonLineString, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes with one malformed in geojson format
        sourceValue = List.of(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in wkt format
        assertEquals(List.of(), fetchSourceValue(mapper, wktMalformed, null));
        assertEquals(List.of(), fetchSourceValue(mapper, wktMalformed, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = List.of(wktLineString, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes with one malformed in wkt format
        sourceValue = List.of(wktLineString, wktMalformed, wktPoint);
        assertEquals(List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

    }
}
