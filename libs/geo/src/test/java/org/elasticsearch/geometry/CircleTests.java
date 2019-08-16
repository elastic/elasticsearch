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

import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

public class CircleTests extends BaseGeometryTestCase<Circle> {
    @Override
    protected Circle createTestInstance(boolean hasAlt) {
        if (hasAlt) {
            return new Circle(randomDoubleBetween(-180, 180, true), randomDoubleBetween(-90, 90, true), randomDouble(),
                randomDoubleBetween(0, 100, false));
            } else {
            return new Circle(randomDoubleBetween(-180, 180, true), randomDoubleBetween(-90, 90, true), randomDoubleBetween(0, 100, false));
        }
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("circle (20.0 10.0 15.0)", wkt.toWKT(new Circle(20, 10, 15)));
        assertEquals(new Circle(20, 10, 15), wkt.fromWKT("circle (20.0 10.0 15.0)"));

        assertEquals("circle (20.0 10.0 15.0 25.0)", wkt.toWKT(new Circle(20, 10, 25, 15)));
        assertEquals(new Circle(20, 10, 25, 15), wkt.fromWKT("circle (20.0 10.0 15.0 25.0)"));

        assertEquals("circle EMPTY", wkt.toWKT(Circle.EMPTY));
        assertEquals(Circle.EMPTY, wkt.fromWKT("circle EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = new GeographyValidator(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Circle(20, 10, -1)));
        assertEquals("Circle radius [-1.0] cannot be negative", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Circle(20, 100, 1)));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Circle(200, 10, 1)));
        assertEquals("invalid longitude 200.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(new Circle(200, 10, 1, 20)));
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new Circle(200, 10, 1, 20));
    }
}
