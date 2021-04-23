/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeCoordinateEncoderTests extends ESTestCase {

    public void testLongitude() {
        double randomLon = randomDoubleBetween(-180, 180, true);
        double randomInvalidLon = randomFrom(randomDoubleBetween(-1000, -180.01, true),
            randomDoubleBetween(180.01, 1000, true));

        assertThat(CoordinateEncoder.GEO.encodeX(Double.POSITIVE_INFINITY), equalTo(Integer.MAX_VALUE));
        assertThat(CoordinateEncoder.GEO.encodeX(Double.NEGATIVE_INFINITY), equalTo(Integer.MIN_VALUE));
        int encodedLon = CoordinateEncoder.GEO.encodeX(randomLon);
        assertThat(encodedLon, equalTo(GeoEncodingUtils.encodeLongitude(randomLon)));
        Exception e = expectThrows(IllegalArgumentException.class, () -> CoordinateEncoder.GEO.encodeX(randomInvalidLon));
        assertThat(e.getMessage(), endsWith("must be between -180.0 and 180.0"));

        assertThat(CoordinateEncoder.GEO.decodeX(encodedLon), closeTo(randomLon, 0.0001));
        assertThat(CoordinateEncoder.GEO.decodeX(Integer.MAX_VALUE), closeTo(180, 0.00001));
        assertThat(CoordinateEncoder.GEO.decodeX(Integer.MIN_VALUE), closeTo(-180, 0.00001));
    }

    public void testLatitude() {
        double randomLat = randomDoubleBetween(-90, 90, true);
        double randomInvalidLat = randomFrom(randomDoubleBetween(-1000, -90.01, true),
            randomDoubleBetween(90.01, 1000, true));

        assertThat(CoordinateEncoder.GEO.encodeY(Double.POSITIVE_INFINITY), equalTo(Integer.MAX_VALUE));
        assertThat(CoordinateEncoder.GEO.encodeY(Double.NEGATIVE_INFINITY), equalTo(Integer.MIN_VALUE));
        int encodedLat = CoordinateEncoder.GEO.encodeY(randomLat);
        assertThat(encodedLat, equalTo(GeoEncodingUtils.encodeLatitude(randomLat)));
        Exception e = expectThrows(IllegalArgumentException.class, () -> CoordinateEncoder.GEO.encodeY(randomInvalidLat));
        assertThat(e.getMessage(), endsWith("must be between -90.0 and 90.0"));

        assertThat(CoordinateEncoder.GEO.decodeY(encodedLat), closeTo(randomLat, 0.0001));
        assertThat(CoordinateEncoder.GEO.decodeY(Integer.MAX_VALUE), closeTo(90, 0.00001));
        assertThat(CoordinateEncoder.GEO.decodeY(Integer.MIN_VALUE), closeTo(-90, 0.00001));
    }
}
