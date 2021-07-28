/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.vectortile.SpatialVectorTileExtension;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GeoShapeWithDocValuesFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper
            = new GeoShapeFieldMapper.Builder("field", true, true).build(new ContentPath()).fieldType();

        Map<String, Object> jsonLineString = org.elasticsearch.core.Map.of("type", "LineString", "coordinates",
            org.elasticsearch.core.List.of(org.elasticsearch.core.List.of(42.0, 27.1), org.elasticsearch.core.List.of(30.0, 50.0)));
        Map<String, Object> jsonPoint = org.elasticsearch.core.Map.of("type", "Point", "coordinates",
            org.elasticsearch.core.List.of(14.0, 15.0));
        Map<String, Object> jsonMalformed = org.elasticsearch.core.Map.of("type", "Point", "coordinates", "foo");
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";
        String wktMalformed = "POINT foo";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(org.elasticsearch.core.List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed single shape in geojson format
        sourceValue = jsonMalformed;
        assertEquals(org.elasticsearch.core.List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = org.elasticsearch.core.List.of(jsonLineString, jsonPoint);
        assertEquals(org.elasticsearch.core.List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in geojson format
        sourceValue = org.elasticsearch.core.List.of(jsonLineString, jsonMalformed, jsonPoint);
        assertEquals(org.elasticsearch.core.List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(org.elasticsearch.core.List.of(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single malformed shape in wkt format
        sourceValue = wktMalformed;
        assertEquals(org.elasticsearch.core.List.of(), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = org.elasticsearch.core.List.of(wktLineString, wktPoint);
        assertEquals(org.elasticsearch.core.List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes including one malformed in wkt format
        sourceValue = org.elasticsearch.core.List.of(wktLineString, wktMalformed, wktPoint);
        assertEquals(org.elasticsearch.core.List.of(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(org.elasticsearch.core.List.of(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    public void testFetchVectorTile() throws IOException {
        fetchVectorTile(GeometryTestUtils.randomPoint());
        fetchVectorTile(GeometryTestUtils.randomMultiPoint(false));
        fetchVectorTile(GeometryTestUtils.randomRectangle());
        fetchVectorTile(GeometryTestUtils.randomLine(false));
        fetchVectorTile(GeometryTestUtils.randomMultiLine(false));
        fetchVectorTile(GeometryTestUtils.randomPolygon(false));
        fetchVectorTile(GeometryTestUtils.randomMultiPolygon(false));
    }

    private void fetchVectorTile(Geometry geometry) throws IOException {
        final MappedFieldType mapper
            = new GeoShapeWithDocValuesFieldMapper.Builder("field", Version.CURRENT, false, false, new SpatialVectorTileExtension())
            .build(new ContentPath()).fieldType();
        final int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        final FeatureFactory featureFactory;
        final String mvtString;
        if (randomBoolean()) {
            int extent = randomIntBetween(1 << 8, 1 << 14);
            mvtString = "mvt(" + z + "/" + x + "/" + y + "@" + extent + ")";
            featureFactory = new FeatureFactory(z, x, y, extent);
        } else {
            mvtString = "mvt(" + z + "/" + x + "/" + y + ")";
            featureFactory = new FeatureFactory(z, x, y, 4096);
        }

        final List<?> sourceValue = fetchSourceValue(mapper, WellKnownText.toWKT(geometry), mvtString);
        final List<byte[]> features = featureFactory.getFeatures(geometry);
        assertThat(features.size(), Matchers.equalTo(sourceValue.size()));
        for (int i = 0; i < features.size(); i++) {
            assertThat(sourceValue.get(i), Matchers.equalTo(features.get(i)));
        }
    }
}
