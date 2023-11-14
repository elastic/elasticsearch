/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class SpatialUtilsTests extends ESTestCase {

    public void testEncoding() {
        for (int i = 0; i < 10; i++) {
            GeoPoint geoPoint = randomGeoPoint();
            GeoPoint point = SpatialUtils.longAsGeoPoint(SpatialUtils.geoPointAsLong(geoPoint));
            assertThat("Latitude[" + i + "]", point.lat(), closeTo(geoPoint.lat(), 1e-5));
            assertThat("Longitude[" + i + "]", point.lon(), closeTo(geoPoint.lon(), 1e-5));
        }
    }

    public void testParsing() {
        for (int i = 0; i < 10; i++) {
            GeoPoint geoPoint = randomGeoPoint();
            GeoPoint point = SpatialUtils.stringAsGeoPoint(SpatialUtils.geoPointAsString(geoPoint));
            assertThat("Latitude[" + i + "]", point.lat(), closeTo(geoPoint.lat(), 1e-5));
            assertThat("Longitude[" + i + "]", point.lon(), closeTo(geoPoint.lon(), 1e-5));
        }
    }
}
