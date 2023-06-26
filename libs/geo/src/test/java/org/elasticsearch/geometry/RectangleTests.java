/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

public class RectangleTests extends BaseGeometryTestCase<Rectangle> {
    @Override
    protected Rectangle createTestInstance(boolean hasAlt) {
        assumeFalse("3rd dimension is not supported yet", hasAlt);
        return GeometryTestUtils.randomRectangle();
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals("BBOX (10.0, 20.0, 40.0, 30.0)", WellKnownText.toWKT(new Rectangle(10, 20, 40, 30)));
        assertEquals(new Rectangle(10, 20, 40, 30), WellKnownText.fromWKT(validator, true, "BBOX (10.0, 20.0, 40.0, 30.0)"));

        assertEquals("BBOX EMPTY", WellKnownText.toWKT(Rectangle.EMPTY));
        assertEquals(Rectangle.EMPTY, WellKnownText.fromWKT(validator, true, "BBOX EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = GeographyValidator.instance(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Rectangle(2, 3, 100, 1)));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Rectangle(200, 3, 2, 1)));
        assertEquals("invalid longitude 200.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Rectangle(2, 3, 1, 2)));
        assertEquals("max y cannot be less than min y", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Rectangle(2, 3, 2, 1, 5, Double.NaN)));
        assertEquals("only one z value is specified", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> StandardValidator.instance(false).validate(new Rectangle(50, 10, 40, 30, 20, 60))
        );
        assertEquals("found Z value [20.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(new Rectangle(50, 10, 40, 30, 20, 60));
    }

    @Override
    protected Rectangle mutateInstance(Rectangle instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
