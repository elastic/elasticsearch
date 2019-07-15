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

package org.elasticsearch.geo.geometry;

import org.elasticsearch.geo.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

public class GeometryCollectionTests extends BaseGeometryTestCase<GeometryCollection<Geometry>> {
    @Override
    protected GeometryCollection<Geometry> createTestInstance(boolean hasAlt) {
        return randomGeometryCollection(hasAlt);
    }



    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, true);
        assertEquals("geometrycollection (point (20.0 10.0),point EMPTY)",
            wkt.toWKT(new GeometryCollection<Geometry>(Arrays.asList(new Point(10, 20), Point.EMPTY))));

        assertEquals(new GeometryCollection<Geometry>(Arrays.asList(new Point(10, 20), Point.EMPTY)),
            wkt.fromWKT("geometrycollection (point (20.0 10.0),point EMPTY)"));

        assertEquals("geometrycollection EMPTY", wkt.toWKT(GeometryCollection.EMPTY));
        assertEquals(GeometryCollection.EMPTY, wkt.fromWKT("geometrycollection EMPTY)"));
    }

    @SuppressWarnings("ConstantConditions")
    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(Collections.emptyList()));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(null));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(
            Arrays.asList(new Point(10, 20), new Point(10, 20, 30))));
        assertEquals("all elements of the collection should have the same number of dimension", ex.getMessage());
    }
}
