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

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.fielddata.ScriptDocValues.GeoPoints;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.contains;

public class ScriptDocValuesGeoPointsTests extends ESTestCase {

    private static MultiGeoPointValues wrap(final GeoPoint... points) {
        return new MultiGeoPointValues() {
            int docID = -1;
            int i;

            @Override
            public GeoPoint nextValue() {
                if (docID != 0) {
                    fail();
                }
                return points[i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                docID = docId;
                return points.length > 0;
            }

            @Override
            public int docValueCount() {
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

    public void testGeoGetLatLon() throws IOException {
        final double lat1 = randomLat();
        final double lat2 = randomLat();
        final double lon1 = randomLon();
        final double lon2 = randomLon();

        Set<String> warnings = new HashSet<>();
        Set<String> keys = new HashSet<>();

        final MultiGeoPointValues values = wrap(new GeoPoint(lat1, lon1), new GeoPoint(lat2, lon2));
        final ScriptDocValues.GeoPoints script = geoPointsWrap(values, (deprecationKey, deprecationMessage)  -> {
            keys.add(deprecationKey);
            warnings.add(deprecationMessage);

            // Create a temporary directory to prove we are running with the server's permissions.
            createTempDir();
        });
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

        /*
         * Invoke getValues() without any permissions to verify it still works.
         * This is done using the callback created above, which creates a temp
         * directory, which is not possible with "noPermission".
         */
        PermissionCollection noPermissions = new Permissions();
        AccessControlContext noPermissionsAcc = new AccessControlContext(
            new ProtectionDomain[] {
                new ProtectionDomain(null, noPermissions)
            }
        );
        AccessController.doPrivileged(new PrivilegedAction<Void>(){
            public Void run() {
                script.getValues();
                return null;
            }
        }, noPermissionsAcc);

        assertThat(warnings, contains(
            "Deprecated getValues used, the field is a list and should be accessed directly."
           + " For example, use doc['foo'] instead of doc['foo'].values."));
        assertThat(keys, contains("ScriptDocValues#getValues"));

    }

    public void testGeoDistance() throws IOException {
        final double lat = randomLat();
        final double lon = randomLon();
        final MultiGeoPointValues values = wrap(new GeoPoint(lat, lon));
        final ScriptDocValues.GeoPoints script = new ScriptDocValues.GeoPoints(values);
        script.setNextDocId(0);

        final ScriptDocValues.GeoPoints emptyScript = new ScriptDocValues.GeoPoints(wrap());
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

    private GeoPoints geoPointsWrap(MultiGeoPointValues in, BiConsumer<String, String> deprecationHandler) {
        return new GeoPoints(in, deprecationHandler);
    }

}
