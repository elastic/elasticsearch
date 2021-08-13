/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.index.fielddata.ScriptDocValues.GeoPoints;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class ScriptDocValuesGeoPointsTests extends ESTestCase {

    private static MultiGeoPointValues wrap(GeoPoint[][] points) {
        return new MultiGeoPointValues() {
            GeoPoint[] current;
            int i;

            @Override
            public GeoPoint nextValue() {
                return current[i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                if (docId < points.length) {
                    current = points[docId];
                } else {
                    current = new GeoPoint[0];
                }
                i = 0;
                return current.length > 0;
            }

            @Override
            public int docValueCount() {
                return current.length;
            }
        };
    }

    private static double randomLat() {
        return randomDouble() * 180 - 90;
    }

    private static double randomLon() {
        return randomDouble() * 360 - 180;
    }

    public void testGeoGetLatLon() throws IOException {
        final double lat1 = randomLat();
        final double lat2 = randomLat();
        final double lon1 = randomLon();
        final double lon2 = randomLon();

        GeoPoint[][] points = {{new GeoPoint(lat1, lon1), new GeoPoint(lat2, lon2)}};
        final MultiGeoPointValues values = wrap(points);
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);

        script.setNextDocId(1);
        assertEquals(true, script.isEmpty());
        script.setNextDocId(0);
        assertEquals(false, script.isEmpty());
        assertEquals(new GeoPoint(lat1, lon1), script.getValue());
        assertEquals(lat1, script.getLat(), 0);
        assertEquals(lon1, script.getLon(), 0);
        assertTrue(Arrays.equals(new double[] {lat1, lat2}, script.getLats()));
        assertTrue(Arrays.equals(new double[] {lon1, lon2}, script.getLons()));
    }

    public void testGeoDistance() throws IOException {
        final double lat = randomLat();
        final double lon = randomLon();
        GeoPoint[][] points = {{new GeoPoint(lat, lon)}};
        final MultiGeoPointValues values = wrap(points);
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);
        script.setNextDocId(0);

        GeoPoint[][] points2 = {new GeoPoint[0]};
        final ScriptDocValues.GeoPoints emptyScript = new ScriptDocValues.GeoPoints(wrap(points2));
        emptyScript.setNextDocId(0);

        final double otherLat = randomLat();
        final double otherLon = randomLon();

        assertEquals(GeoUtils.arcDistance(lat, lon, otherLat, otherLon) / 1000d,
                script.arcDistance(otherLat, otherLon) / 1000d, 0.01);
        assertEquals(GeoUtils.arcDistance(lat, lon, otherLat, otherLon) / 1000d,
                script.arcDistanceWithDefault(otherLat, otherLon, 42) / 1000d, 0.01);
        assertEquals(42, emptyScript.arcDistanceWithDefault(otherLat, otherLon, 42), 0);

        assertEquals(GeoUtils.planeDistance(lat, lon, otherLat, otherLon) / 1000d,
                script.planeDistance(otherLat, otherLon) / 1000d, 0.01);
        assertEquals(GeoUtils.planeDistance(lat, lon, otherLat, otherLon) / 1000d,
                script.planeDistanceWithDefault(otherLat, otherLon, 42) / 1000d, 0.01);
        assertEquals(42, emptyScript.planeDistanceWithDefault(otherLat, otherLon, 42), 0);
    }

    public void testMissingValues() throws IOException {
        GeoPoint[][] points = new GeoPoint[between(3, 10)][];
        for (int d = 0; d < points.length; d++) {
            points[d] = new GeoPoint[randomBoolean() ? 0 : between(1, 10)];
            for (int i = 0; i< points[d].length; i++) {
                points[d][i] =  new GeoPoint(randomLat(), randomLon());
            }
        }
        final ScriptDocValues.GeoPoints geoPoints = new GeoPoints(wrap(points));
        for (int d = 0; d < points.length; d++) {
            geoPoints.setNextDocId(d);
            if (points[d].length > 0) {
                assertEquals(points[d][0], geoPoints.getValue());
            } else {
                Exception e = expectThrows(IllegalStateException.class, () -> geoPoints.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
                e = expectThrows(IllegalStateException.class, () -> geoPoints.get(0));
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            }
            assertEquals(points[d].length, geoPoints.size());
            for (int i = 0; i < points[d].length; i++) {
                assertEquals(points[d][i], geoPoints.get(i));
            }
        }
    }


}
