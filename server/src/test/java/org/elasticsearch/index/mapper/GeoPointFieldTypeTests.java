/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.script.ScriptCompiler;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper
            = new GeoPointFieldMapper.Builder("field", ScriptCompiler.NONE, false).build(new ContentPath()).fieldType();

        Map<String, Object> jsonPoint = new HashMap<>();
        jsonPoint.put("type", "Point");
        jsonPoint.put("coordinates", Arrays.asList(42.0, 27.1));
        Map<String, Object> otherJsonPoint = new HashMap<>();
        otherJsonPoint.put("type", "Point");
        otherJsonPoint.put("coordinates", Arrays.asList(30.0, 50.0));
        String wktPoint = "POINT (42.0 27.1)";
        String otherWktPoint = "POINT (30.0 50.0)";

        // Test a single point in [lon, lat] array format.
        Object sourceValue = Arrays.asList(42.0, 27.1);
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in "lat, lon" string format.
        sourceValue = "27.1,42.0";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [lon, lat] array format.
        sourceValue = Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0));
        assertEquals(Arrays.asList(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in [lat,lon] array format with one malformed
        sourceValue = Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList("a", "b"), Arrays.asList(30.0, 50.0));
        assertEquals(Arrays.asList(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(Collections.singletonList(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a malformed value
        sourceValue = "malformed";
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, sourceValue, null));
    }

    public void testFetchVectorTile() throws IOException {
        MappedFieldType mapper
            = new GeoPointFieldMapper.Builder("field", ScriptCompiler.NONE, false).build(new ContentPath()).fieldType();
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
            List<?> sourceValue = fetchSourceValue(mapper, org.elasticsearch.core.List.of(lon, lat), mvtString);
            assertThat(sourceValue.size(), Matchers.equalTo(1));
            assertThat(sourceValue.get(0), Matchers.equalTo(featureFactory.point(lon, lat)));
            geoPoints.add(new GeoPoint(lat, lon));
            values.add(org.elasticsearch.core.List.of(lon, lat));
        }
        List<?> sourceValue = fetchSourceValue(mapper, values, mvtString);
        assertThat(sourceValue.size(), Matchers.equalTo(1));
        assertThat(sourceValue.get(0), Matchers.equalTo(featureFactory.points(geoPoints)));
    }
}
