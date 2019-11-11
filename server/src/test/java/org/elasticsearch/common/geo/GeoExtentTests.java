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
package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

/**
 * Basic Tests for {@link GeoExtent}
 */
public class GeoExtentTests extends AbstractWireSerializingTestCase<GeoExtent> {

    @Override
    protected GeoExtent createTestInstance() {
        GeoExtent extent = new GeoExtent();
        // we occasionally want to test empty extents
        if (frequently()) {
            int numPoints = randomIntBetween(2, 100);
            for (int i = 0; i < numPoints; i++) {
                extent.addPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
            }
        }
        return extent;
    }

    @Override
    protected Writeable.Reader<GeoExtent> instanceReader() {
        return GeoExtent::new;
    }

    public void testExtentSerialization() throws IOException  {
        GeoExtent extent = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            extent.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                GeoExtent copy = new GeoExtent(in);
                assertEquals(extent.toString() + " vs. " + copy.toString(), extent, copy);
            }
        }
    }

    public void testSinglePoint() {
        GeoExtent extent = new GeoExtent();
        double lat = GeoTestUtil.nextLatitude();
        double lon = GeoTestUtil.nextLongitude();
        extent.addPoint(lat, lon);
        assertEquals(extent.minLat(), lat, 0.0);
        assertEquals(extent.maxLat(), lat, 0.0);
        assertEquals(extent.minLon(false), lon, 0.0);
        assertEquals(extent.maxLon(false), lon, 0.0);
        assertEquals(extent.minLon(true), lon, 0.0);
        assertEquals(extent.maxLon(true), lon, 0.0);
    }

    public void testAddExtent() {
        GeoExtent extent = createTestInstance();
        GeoExtent copy = new GeoExtent();
        copy.addExtent(extent);
        assertEquals(extent.toString() + " vs. " + copy.toString(), extent, copy);
    }

    public void testEmptyExtent() {
        GeoExtent extent = new GeoExtent();
        assertEquals(extent.minLat(), Double.POSITIVE_INFINITY, 0.0);
        assertEquals(extent.maxLat(), Double.NEGATIVE_INFINITY, 0.0);
        assertEquals(extent.minLon(false), Double.POSITIVE_INFINITY, 0.0);
        assertEquals(extent.maxLon(false), Double.NEGATIVE_INFINITY, 0.0);
        assertEquals(extent.minLon(true), Double.POSITIVE_INFINITY, 0.0);
        assertEquals(extent.maxLon(true), Double.NEGATIVE_INFINITY, 0.0);
    }

    public void testWrapLongitude() {
        {
            // longitude should be wrapped
            GeoExtent extent = new GeoExtent();
            int numPoints = randomIntBetween(2, 100);
            for (int i = 0; i < numPoints; i++) {
                double lon1 = GeoTestUtil.nextLongitude();
                while (lon1 > -90) {
                    lon1 = GeoTestUtil.nextLongitude();
                }
                extent.addPoint(GeoTestUtil.nextLatitude(), lon1);
                double lon2 = GeoTestUtil.nextLongitude();
                while (lon2 < 90) {
                    lon2 = GeoTestUtil.nextLongitude();
                }
                extent.addPoint(GeoTestUtil.nextLatitude(), lon2);
            }
            assertTrue(extent.maxLat() >= extent.minLat());
            assertTrue(extent.minLon(true) > extent.maxLon(true));
            assertTrue(extent.maxLon(false) > extent.minLon(false));
        }
        {
            // longitude should not be wrapped
            GeoExtent extent = new GeoExtent();
            int numPoints = randomIntBetween(2, 100);
            for (int i = 0; i < numPoints; i++) {
                double lon = GeoTestUtil.nextLongitude();
                while (lon < -90 || lon > 90) {
                    lon = GeoTestUtil.nextLongitude();
                }
                extent.addPoint(GeoTestUtil.nextLatitude(), lon);
            }
            assertTrue(extent.maxLat() >= extent.minLat());
            assertTrue(extent.maxLon(true) > extent.minLon(true));
            assertTrue(extent.maxLon(false) > extent.minLon(false));
        }
    }
}
