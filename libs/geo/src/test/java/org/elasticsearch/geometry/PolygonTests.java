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

package org.elasticsearch.geometry;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

public class PolygonTests extends BaseGeometryTestCase<Polygon> {
    @Override
    protected Polygon createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomPolygon(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("POLYGON ((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0))",
            wkt.toWKT(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}))));
        assertEquals(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1})),
            wkt.fromWKT("POLYGON ((3 1, 4 2, 5 3, 3 1))"));

        assertEquals("POLYGON ((3.0 1.0 5.0, 4.0 2.0 4.0, 5.0 3.0 3.0, 3.0 1.0 5.0))",
            wkt.toWKT(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{5, 4, 3, 5}))));
        assertEquals(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{5, 4, 3, 5})),
            wkt.fromWKT("POLYGON ((3 1 5, 4 2 4, 5 3 3, 3 1 5))"));

        // Auto closing in coerce mode
        assertEquals(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1})),
            wkt.fromWKT("POLYGON ((3 1, 4 2, 5 3))"));
        assertEquals(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{5, 4, 3, 5})),
            wkt.fromWKT("POLYGON ((3 1 5, 4 2 4, 5 3 3))"));
        assertEquals(new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}),
            Collections.singletonList(new LinearRing(new double[]{0.5, 2.5, 2.0, 0.5}, new double[]{1.5, 1.5, 1.0, 1.5}))),
            wkt.fromWKT("POLYGON ((3 1, 4 2, 5 3, 3 1), (0.5 1.5, 2.5 1.5, 2.0 1.0))"));

        assertEquals("POLYGON EMPTY", wkt.toWKT(Polygon.EMPTY));
        assertEquals(Polygon.EMPTY, wkt.fromWKT("POLYGON EMPTY)"));
    }

    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[]{3, 4, 3}, new double[]{1, 2, 1})));
        assertEquals("at least 4 polygon points required", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}), null));
        assertEquals("holes must not be null", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{5, 4, 3, 5}),
                Collections.singletonList(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}))));
        assertEquals("holes must have the same number of dimensions as the polygon", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1}))));
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1})));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new WellKnownText(false, new GeographyValidator(true)).fromWKT("polygon ((3 1 5, 4 2 4, 5 3 3))"));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): " +
            "x[0]=3.0 x[2]=5.0 y[0]=1.0 y[2]=3.0 z[0]=5.0 z[2]=3.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new WellKnownText(randomBoolean(), new GeographyValidator(false)).fromWKT("polygon ((3 1 5, 4 2 4, 5 3 3, 3 1 5))"));
        assertEquals("found Z value [5.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> new WellKnownText(false, new GeographyValidator(randomBoolean())).fromWKT(
                "polygon ((3 1, 4 2, 5 3, 3 1), (0.5 1.5, 2.5 1.5, 2.0 1.0))"));
        assertEquals("first and last points of the linear ring must be the same (it must close itself): " +
            "x[0]=0.5 x[2]=2.0 y[0]=1.5 y[2]=1.0", ex.getMessage());
    }
}
