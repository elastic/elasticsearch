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

public class PointTests extends BaseGeometryTestCase<Point> {
    @Override
    protected Point createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomPoint(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("POINT (20.0 10.0)", wkt.toWKT(new Point(20, 10)));
        assertEquals(new Point(20, 10), wkt.fromWKT("point (20.0 10.0)"));

        assertEquals("POINT (20.0 10.0 100.0)", wkt.toWKT(new Point(20, 10, 100)));
        assertEquals(new Point(20, 10, 100), wkt.fromWKT("POINT (20.0 10.0 100.0)"));

        assertEquals("POINT EMPTY", wkt.toWKT(Point.EMPTY));
        assertEquals(Point.EMPTY, wkt.fromWKT("POINT EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = new GeographyValidator(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Point(10, 100)));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Point(500, 10)));
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(new Point(2, 1, 3)));
        assertEquals("found Z value [3.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new Point(2, 1, 3));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new WellKnownText(randomBoolean(), new GeographyValidator(false)).fromWKT("point (20.0 10.0 100.0)"));
        assertEquals("found Z value [100.0] but [ignore_z_value] parameter is [false]", ex.getMessage());
    }
}
