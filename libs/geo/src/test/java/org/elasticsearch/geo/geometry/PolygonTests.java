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

public class PolygonTests extends BaseGeometryTestCase<Polygon> {
    @Override
    protected Polygon createTestInstance() {
        return randomPolygon();
    }

    public void testBasicSerialization() throws IOException, ParseException {
        assertEquals("polygon ((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0))",
            WellKnownText.toWKT(new Polygon(new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 5, 3}))));
        assertEquals(new Polygon(new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 5, 3})),
            WellKnownText.fromWKT("polygon ((3 1, 4 2, 5 3, 3 1))"));

        assertEquals("polygon EMPTY", WellKnownText.toWKT(Polygon.EMPTY));
        assertEquals(Polygon.EMPTY, WellKnownText.fromWKT("polygon EMPTY)"));
    }

    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[]{1, 2, 1}, new double[]{3, 4, 3})));
        assertEquals("at least 4 polygon points required", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[]{1, 2, 3, 1}, new double[]{3, 4, 5, 3}), null));
        assertEquals("holes must not be null", ex.getMessage());
    }
}
