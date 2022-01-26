/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Basic Tests for {@link GeoDistance}
 */
public class GeoDistanceTests extends ESTestCase {

    public void testGeoDistanceSerialization() throws IOException {
        // make sure that ordinals don't change, because we rely on then in serialization
        assertThat(GeoDistance.PLANE.ordinal(), equalTo(0));
        assertThat(GeoDistance.ARC.ordinal(), equalTo(1));
        assertThat(GeoDistance.values().length, equalTo(2));

        GeoDistance geoDistance = randomFrom(GeoDistance.PLANE, GeoDistance.ARC);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            geoDistance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                GeoDistance copy = GeoDistance.readFromStream(in);
                assertEquals(copy.toString() + " vs. " + geoDistance.toString(), copy, geoDistance);
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            if (randomBoolean()) {
                out.writeVInt(randomIntBetween(GeoDistance.values().length, Integer.MAX_VALUE));
            } else {
                out.writeVInt(randomIntBetween(Integer.MIN_VALUE, -1));
            }
            try (StreamInput in = out.bytes().streamInput()) {
                GeoDistance.readFromStream(in);
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown GeoDistance ordinal ["));
            }
        }
    }

    private static double arcDistance(GeoPoint p1, GeoPoint p2) {
        return GeoDistance.ARC.calculate(p1.lat(), p1.lon(), p2.lat(), p2.lon(), DistanceUnit.METERS);
    }

    private static double planeDistance(GeoPoint p1, GeoPoint p2) {
        return GeoDistance.PLANE.calculate(p1.lat(), p1.lon(), p2.lat(), p2.lon(), DistanceUnit.METERS);
    }

    public void testArcDistanceVsPlane() {
        // sameLongitude and sameLatitude are both 90 degrees away from basePoint along great circles
        final GeoPoint basePoint = new GeoPoint(45, 90);
        final GeoPoint sameLongitude = new GeoPoint(-45, 90);
        final GeoPoint sameLatitude = new GeoPoint(45, -90);

        double sameLongitudeArcDistance = arcDistance(basePoint, sameLongitude);
        double sameLatitudeArcDistance = arcDistance(basePoint, sameLatitude);
        double sameLongitudePlaneDistance = planeDistance(basePoint, sameLongitude);
        double sameLatitudePlaneDistance = planeDistance(basePoint, sameLatitude);

        // GeoDistance.PLANE measures the distance along a straight line in
        // (lat, long) space so agrees with GeoDistance.ARC along a line of
        // constant longitude but takes a longer route if there is east/west
        // movement.

        assertThat(
            "Arc and plane should agree on sameLongitude",
            Math.abs(sameLongitudeArcDistance - sameLongitudePlaneDistance),
            lessThan(0.001)
        );

        assertThat(
            "Arc and plane should disagree on sameLatitude (by >4000km)",
            sameLatitudePlaneDistance - sameLatitudeArcDistance,
            greaterThan(4.0e6)
        );

        // GeoDistance.ARC calculates the great circle distance (on a sphere) so these should agree as they're both 90 degrees
        assertThat("Arc distances should agree", Math.abs(sameLongitudeArcDistance - sameLatitudeArcDistance), lessThan(0.001));
    }

    public void testArcDistanceVsPlaneAccuracy() {
        // These points only differ by a few degrees so the calculation methods
        // should match more closely. Check that the deviation is small enough,
        // but not too small.

        // The biggest deviations are away from the equator and the poles so pick a suitably troublesome latitude.
        GeoPoint basePoint = new GeoPoint(randomDoubleBetween(30.0, 60.0, true), randomDoubleBetween(-180.0, 180.0, true));
        GeoPoint sameLongitude = new GeoPoint(randomDoubleBetween(-90.0, 90.0, true), basePoint.lon());
        GeoPoint sameLatitude = new GeoPoint(basePoint.lat(), basePoint.lon() + randomDoubleBetween(4.0, 10.0, true));

        double sameLongitudeArcDistance = arcDistance(basePoint, sameLongitude);
        double sameLatitudeArcDistance = arcDistance(basePoint, sameLatitude);
        double sameLongitudePlaneDistance = planeDistance(basePoint, sameLongitude);
        double sameLatitudePlaneDistance = planeDistance(basePoint, sameLatitude);

        assertThat(
            "Arc and plane should agree [" + basePoint + "] to [" + sameLongitude + "] (within 1cm)",
            Math.abs(sameLongitudeArcDistance - sameLongitudePlaneDistance),
            lessThan(0.01)
        );

        assertThat(
            "Arc and plane should very roughly agree [" + basePoint + "] to [" + sameLatitude + "]",
            sameLatitudePlaneDistance - sameLatitudeArcDistance,
            lessThan(600.0)
        );

        assertThat(
            "Arc and plane should disagree by some margin [" + basePoint + "] to [" + sameLatitude + "]",
            sameLatitudePlaneDistance - sameLatitudeArcDistance,
            greaterThan(15.0)
        );
    }
}
