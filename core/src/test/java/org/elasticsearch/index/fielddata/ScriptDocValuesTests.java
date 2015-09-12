/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class ScriptDocValuesTests extends ESTestCase {

    private static MultiGeoPointValues wrap(final GeoPoint... points) {
        return new MultiGeoPointValues() {
            int docID = -1;

            @Override
            public GeoPoint valueAt(int i) {
                if (docID != 0) {
                    fail();
                }
                return points[i];
            }
            
            @Override
            public void setDocument(int docId) {
                this.docID = docId;
            }
            
            @Override
            public int count() {
                if (docID != 0) {
                    return 0;
                }
                return points.length;
            }
        };
    }

    private static double randomLat() {
        return randomDouble() * 180 - 90;
    }

    private static double randomLon() {
        return randomDouble() * 360 - 180;
    }

    public void testGeoGetLatLon() {
        final double lat1 = randomLat();
        final double lat2 = randomLat();
        final double lon1 = randomLon();
        final double lon2 = randomLon();
        final MultiGeoPointValues values = wrap(new GeoPoint(lat1, lon1), new GeoPoint(lat2, lon2));
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);
        script.setNextDocId(1);
        assertEquals(true, script.isEmpty());
        script.setNextDocId(0);
        assertEquals(false, script.isEmpty());
        assertEquals(new GeoPoint(lat1, lon1), script.getValue());
        assertEquals(Arrays.asList(new GeoPoint(lat1, lon1), new GeoPoint(lat2, lon2)), script.getValues());
        assertEquals(lat1, script.getLat(), 0);
        assertEquals(lon1, script.getLon(), 0);
        assertTrue(Arrays.equals(new double[] {lat1, lat2}, script.getLats()));
        assertTrue(Arrays.equals(new double[] {lon1, lon2}, script.getLons()));
    }

    public void testGeoDistance() {
        final double lat = randomLat();
        final double lon = randomLon();
        final MultiGeoPointValues values = wrap(new GeoPoint(lat, lon));
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);
        script.setNextDocId(0);

        final ScriptDocValues.GeoPoints emptyScript = new ScriptDocValues.GeoPoints(wrap());
        emptyScript.setNextDocId(0);

        final double otherLat = randomLat();
        final double otherLon = randomLon();
        
        assertEquals(GeoDistance.ARC.calculate(lat, lon, otherLat, otherLon, DistanceUnit.KILOMETERS),
                script.arcDistanceInKm(otherLat, otherLon), 0.01);
        assertEquals(GeoDistance.ARC.calculate(lat, lon, otherLat, otherLon, DistanceUnit.KILOMETERS),
                script.arcDistanceInKmWithDefault(otherLat, otherLon, 42), 0.01);
        assertEquals(42, emptyScript.arcDistanceInKmWithDefault(otherLat, otherLon, 42), 0);

        assertEquals(GeoDistance.PLANE.calculate(lat, lon, otherLat, otherLon, DistanceUnit.KILOMETERS),
                script.distanceInKm(otherLat, otherLon), 0.01);
        assertEquals(GeoDistance.PLANE.calculate(lat, lon, otherLat, otherLon, DistanceUnit.KILOMETERS),
                script.distanceInKmWithDefault(otherLat, otherLon, 42), 0.01);
        assertEquals(42, emptyScript.distanceInKmWithDefault(otherLat, otherLon, 42), 0);
    }

}
