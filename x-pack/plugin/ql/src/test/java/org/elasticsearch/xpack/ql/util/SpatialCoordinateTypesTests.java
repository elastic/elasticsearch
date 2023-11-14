/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class SpatialCoordinateTypesTests extends ESTestCase {

    public void testEncoding() {
        for (int i = 0; i < 10; i++) {
            SpatialPoint geoPoint = randomGeoPoint();
            SpatialPoint point = SpatialCoordinateTypes.Geo.longAsPoint(SpatialCoordinateTypes.Geo.pointAsLong(geoPoint));
            assertThat("Latitude[" + i + "]", point.getY(), closeTo(geoPoint.getY(), 1e-5));
            assertThat("Longitude[" + i + "]", point.getX(), closeTo(geoPoint.getX(), 1e-5));
        }
    }

    public void testParsing() {
        for (int i = 0; i < 10; i++) {
            SpatialPoint geoPoint = randomGeoPoint();
            SpatialPoint point = SpatialCoordinateTypes.Geo.stringAsPoint(SpatialCoordinateTypes.Geo.pointAsString(geoPoint));
            assertThat("Latitude[" + i + "]", point.getY(), closeTo(geoPoint.getY(), 1e-5));
            assertThat("Longitude[" + i + "]", point.getX(), closeTo(geoPoint.getX(), 1e-5));
        }
    }
}
