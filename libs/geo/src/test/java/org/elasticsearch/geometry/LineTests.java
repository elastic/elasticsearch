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
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

public class LineTests extends BaseGeometryTestCase<Line> {
    @Override
    protected Line createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomLine(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("linestring (3.0 1.0, 4.0 2.0)", wkt.toWKT(new Line(new double[]{3, 4}, new double[]{1, 2})));
        assertEquals(new Line(new double[]{3, 4}, new double[]{1, 2}), wkt.fromWKT("linestring (3 1, 4 2)"));

        assertEquals("linestring (3.0 1.0 5.0, 4.0 2.0 6.0)", wkt.toWKT(new Line(new double[]{3, 4}, new double[]{1, 2},
            new double[]{5, 6})));
        assertEquals(new Line(new double[]{3, 4}, new double[]{1, 2}, new double[]{6, 5}),
            wkt.fromWKT("linestring (3 1 6, 4 2 5)"));

        assertEquals("linestring EMPTY", wkt.toWKT(Line.EMPTY));
        assertEquals(Line.EMPTY, wkt.fromWKT("linestring EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = new GeographyValidator(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[]{3}, new double[]{1})));
        assertEquals("at least two points in the line is required", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[]{3, 4, 500, 3}, new double[]{1, 2, 3, 1})));
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[]{3, 4, 5, 3}, new double[]{1, 100, 3, 1})));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new Line(new double[]{3, 4}, new double[]{1, 2}, new double[]{6, 5})));
        assertEquals("found Z value [6.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new Line(new double[]{3, 4}, new double[]{1, 2}, new double[]{6, 5}));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new WellKnownText(randomBoolean(), new GeographyValidator(false)).fromWKT("linestring (3 1 6, 4 2 5)"));
        assertEquals("found Z value [6.0] but [ignore_z_value] parameter is [false]", ex.getMessage());
    }
}
