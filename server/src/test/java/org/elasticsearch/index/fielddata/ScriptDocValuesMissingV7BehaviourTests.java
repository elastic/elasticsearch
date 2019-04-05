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
import org.elasticsearch.index.fielddata.ScriptDocValues.GeoPoints;
import org.elasticsearch.index.fielddata.ScriptDocValues.Booleans;
import org.elasticsearch.index.fielddata.ScriptDocValues.Dates;
import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.test.ESTestCase;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;
import java.io.IOException;

public class ScriptDocValuesMissingV7BehaviourTests extends ESTestCase {

    public void testExceptionForMissingValueLong() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Longs longs = wrap(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longs.setNextDocId(d);
            Exception e = expectThrows(IllegalStateException.class, () -> longs.getValue());
            assertEquals("A document doesn't have a value for a field! " +
                "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
        }
    }

    public void testExceptionForMissingValueDate() throws IOException {
        final ReadableDateTime EPOCH = new DateTime(0, DateTimeZone.UTC);
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Dates dates = wrapDates(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            dates.setNextDocId(d);
            Exception e = expectThrows(IllegalStateException.class, () -> dates.getValue());
            assertEquals("A document doesn't have a value for a field! " +
                "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
        }
    }

    public void testFalseForMissingValueBoolean() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[0];
        }
        Booleans bools = wrapBooleans(values);
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            bools.setNextDocId(d);
            Exception e = expectThrows(IllegalStateException.class, () -> bools.getValue());
            assertEquals("A document doesn't have a value for a field! " +
                "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
        }
    }

    public void testMissingValuesGeo() throws IOException {
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

    private static double randomLat() {
        return randomDouble() * 180 - 90;
    }

    private static double randomLon() {
        return randomDouble() * 360 - 180;
    }


    private Longs wrap(long[][] values) {
        return new Longs(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;
            @Override
            public boolean advanceExact(int doc) {
                i = 0;
                current = values[doc];
                return current.length > 0;
            }
            @Override
            public int docValueCount() {
                return current.length;
            }
            @Override
            public long nextValue() {
                return current[i++];
            }
        });
    }

    private Booleans wrapBooleans(long[][] values) {
        return new Booleans(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;
            @Override
            public boolean advanceExact(int doc) {
                i = 0;
                current = values[doc];
                return current.length > 0;
            }
            @Override
            public int docValueCount() {
                return current.length;
            }
            @Override
            public long nextValue() {
                return current[i++];
            }
        });
    }

    private Dates wrapDates(long[][] values) {
        return new Dates(new AbstractSortedNumericDocValues() {
            long[] current;
            int i;
            @Override
            public boolean advanceExact(int doc) {
                current = values[doc];
                i = 0;
                return current.length > 0;
            }
            @Override
            public int docValueCount() {
                return current.length;
            }
            @Override
            public long nextValue() {
                return current[i++];
            }
        });
    }


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

}
