/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        boolean ignoreMalformed = randomBoolean();
        MappedFieldType mapper = new GeoPointFieldMapper.Builder(
            "field",
            ScriptCompiler.NONE,
            ignoreMalformed,
            IndexVersion.current(),
            null
        ).build(MapperBuilderContext.root(false, false)).fieldType();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String wktPoint = "POINT (42.0 27.1)";
        String otherWktPoint = "POINT (30.0 50.0)";
        byte[] wkbPoint = WellKnownBinary.toWKB(new Point(42.0, 27.1), ByteOrder.LITTLE_ENDIAN);
        byte[] otherWkbPoint = WellKnownBinary.toWKB(new Point(30.0, 50.0), ByteOrder.LITTLE_ENDIAN);

        // Test a single point in [lon, lat] array format.
        Object sourceValue = List.of(42.0, 27.1);
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        List<?> wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a single point in "lat, lon" string format.
        sourceValue = "27.1,42.0";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a list of points in [lon, lat] array format.
        sourceValue = List.of(List.of(42.0, 27.1), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbPoint));
        assertThat(wkb.get(1), equalTo(otherWkbPoint));

        // Test a list of points in [lat,lon] array format with one malformed
        sourceValue = List.of(List.of(42.0, 27.1), List.of("a", "b"), List.of(30.0, 50.0));
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
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a malformed value
        sourceValue = "malformed";
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));

        // test normalize
        sourceValue = "27.1,402.0";
        if (ignoreMalformed) {
            assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
            assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
            wkb = fetchSourceValue(mapper, sourceValue, "wkb");
            assertThat(wkb.size(), equalTo(1));
            assertThat(wkb.get(0), equalTo(wkbPoint));
        } else {
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkb"));
        }

        // test single point in GeoJSON format
        sourceValue = jsonPoint;
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in GeoJSON format
        sourceValue = List.of(jsonPoint, otherJsonPoint);
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    public void testFetchVectorTile() throws IOException {
        MappedFieldType mapper = new GeoPointFieldMapper.Builder("field", ScriptCompiler.NONE, false, IndexVersion.current(), null).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();
        final int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        final SimpleFeatureFactory featureFactory;
        final String mvtString;
        if (randomBoolean()) {
            int extent = randomIntBetween(1 << 8, 1 << 14);
            mvtString = "mvt(" + z + "/" + x + "/" + y + "@" + extent + ")";
            featureFactory = new SimpleFeatureFactory(z, x, y, extent);
        } else {
            mvtString = "mvt(" + z + "/" + x + "/" + y + ")";
            featureFactory = new SimpleFeatureFactory(z, x, y, 4096);
        }
        List<GeoPoint> geoPoints = new ArrayList<>();
        List<List<Double>> values = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final double lat = GeoTestUtil.nextLatitude();
            final double lon = GeoTestUtil.nextLongitude();
            List<?> sourceValue = fetchSourceValue(mapper, List.of(lon, lat), mvtString);
            assertThat(sourceValue.size(), equalTo(1));
            assertThat(sourceValue.get(0), equalTo(featureFactory.point(lon, lat)));
            geoPoints.add(new GeoPoint(lat, lon));
            values.add(List.of(lon, lat));
        }
        List<?> sourceValue = fetchSourceValue(mapper, values, mvtString);
        assertThat(sourceValue.size(), equalTo(1));
        assertThat(sourceValue.get(0), equalTo(featureFactory.points(geoPoints)));
    }
}
