/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class PointTests extends BaseGeometryTestCase<Point> {
    @Override
    protected Point createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomPoint(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals("POINT (20.0 10.0)", WellKnownText.toWKT(new Point(20, 10)));
        assertEquals(new Point(20, 10), WellKnownText.fromWKT(validator, true, "point (20.0 10.0)"));

        assertEquals("POINT (20.0 10.0 100.0)", WellKnownText.toWKT(new Point(20, 10, 100)));
        assertEquals(new Point(20, 10, 100), WellKnownText.fromWKT(validator, true, "POINT (20.0 10.0 100.0)"));

        assertEquals("POINT EMPTY", WellKnownText.toWKT(Point.EMPTY));
        assertEquals(Point.EMPTY, WellKnownText.fromWKT(validator, true, "POINT EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = GeographyValidator.instance(true);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Point(10, 100)));
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> validator.validate(new Point(500, 10)));
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> StandardValidator.instance(false).validate(new Point(2, 1, 3)));
        assertEquals("found Z value [3.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(new Point(2, 1, 3));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(GeographyValidator.instance(false), randomBoolean(), "point (20.0 10.0 100.0)")
        );
        assertEquals("found Z value [100.0] but [ignore_z_value] parameter is [false]", ex.getMessage());
    }

    public void testParsePointZWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals(new Point(20, 10, 100), WellKnownText.fromWKT(validator, true, "POINT Z (20.0 10.0 100.0)"));
    }

    public void testParsePointMWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals(new Point(20, 10, 100), WellKnownText.fromWKT(validator, true, "POINT M (20.0 10.0 100.0)"));
    }

    public void testParsePointZWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = GeographyValidator.instance(true);
        ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(validator, true, "POINT Z (20.0 10.0)"));
        assertEquals("'POINT Z' or 'POINT M' must have three coordinates, but only two were found.", ex.getMessage());
    }

    public void testParsePointMWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = StandardValidator.instance(true);
        ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(validator, true, "POINT M (20.0 10.0)"));
        assertEquals("'POINT Z' or 'POINT M' must have three coordinates, but only two were found.", ex.getMessage());
    }

    public void testParsePointZMFormatNotSupported() {
        GeometryValidator validator = StandardValidator.instance(true);
        List<String> points = List.of("POINT ZM (20.0 10.0 100.0 200.0)", "POINT ZM (20.0 10.0 100.0)", "POINT ZM (20.0 10.0)");
        for (String point : points) {
            ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(validator, true, point));
            assertEquals("expected ( or Z or M but found: ZM", ex.getMessage());
        }
    }

    public void testParsePointZWithEmpty() {
        GeometryValidator validator = StandardValidator.instance(true);
        ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(validator, true, "POINT Z EMPTY"));
        assertEquals("expected ( but found: EMPTY", ex.getMessage());
    }

    public void testParsePointZOrMWithTwoCoordinates() {
        GeometryValidator validator = StandardValidator.instance(true);
        List<String> points = List.of("POINT Z (20.0 10.0)", "POINT M (20.0 10.0)");
        for (String point : points) {
            ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(validator, true, point));
            assertEquals("'POINT Z' or 'POINT M' must have three coordinates, but only two were found.", ex.getMessage());
        }
    }

    @Override
    protected Point mutateInstance(Point instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
