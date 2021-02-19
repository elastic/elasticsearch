/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        assertEquals("CIRCLE (20.0 10.0 15.0)", wkt.toWKT(new Circle(20, 10, 15)));
        assertEquals(new Circle(20, 10, 15), wkt.fromWKT("circle (20.0 10.0 15.0)"));

        assertEquals("CIRCLE (20.0 10.0 15.0 25.0)", wkt.toWKT(new Circle(20, 10, 25, 15)));
        assertEquals(new Circle(20, 10, 25, 15), wkt.fromWKT("circle (20.0 10.0 15.0 25.0)"));

        assertEquals("CIRCLE EMPTY", wkt.toWKT(Circle.EMPTY));
        assertEquals(Circle.EMPTY, wkt.fromWKT("CIRCLE EMPTY)"));
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
