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

public class LineTests extends BaseGeometryTestCase<Line> {
    @Override
    protected Line createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomLine(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals("LINESTRING (3.0 1.0, 4.0 2.0)", WellKnownText.toWKT(new Line(new double[] { 3, 4 }, new double[] { 1, 2 })));
        assertEquals(
            new Line(new double[] { 3, 4 }, new double[] { 1, 2 }),
            WellKnownText.fromWKT(validator, true, "LINESTRING (3 1, 4 2)")
        );

        assertEquals(
            "LINESTRING (3.0 1.0 5.0, 4.0 2.0 6.0)",
            WellKnownText.toWKT(new Line(new double[] { 3, 4 }, new double[] { 1, 2 }, new double[] { 5, 6 }))
        );
        assertEquals(
            new Line(new double[] { 3, 4 }, new double[] { 1, 2 }, new double[] { 6, 5 }),
            WellKnownText.fromWKT(validator, true, "LINESTRING (3 1 6, 4 2 5)")
        );

        assertEquals("LINESTRING EMPTY", WellKnownText.toWKT(Line.EMPTY));
        assertEquals(Line.EMPTY, WellKnownText.fromWKT(validator, true, "LINESTRING EMPTY)"));
    }

    public void testInitValidation() {
        GeometryValidator validator = GeographyValidator.instance(true);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[] { 3 }, new double[] { 1 }))
        );
        assertEquals("at least two points in the line is required", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[] { 3, 4, 500, 3 }, new double[] { 1, 2, 3, 1 }))
        );
        assertEquals("invalid longitude 500.0; must be between -180.0 and 180.0", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(new Line(new double[] { 3, 4, 5, 3 }, new double[] { 1, 100, 3, 1 }))
        );
        assertEquals("invalid latitude 100.0; must be between -90.0 and 90.0", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> StandardValidator.instance(false).validate(new Line(new double[] { 3, 4 }, new double[] { 1, 2 }, new double[] { 6, 5 }))
        );
        assertEquals("found Z value [6.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(new Line(new double[] { 3, 4 }, new double[] { 1, 2 }, new double[] { 6, 5 }));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(GeographyValidator.instance(false), randomBoolean(), "linestring (3 1 6, 4 2 5)")
        );
        assertEquals("found Z value [6.0] but [ignore_z_value] parameter is [false]", ex.getMessage());
    }

    public void testParseLineZorMWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);

        Line expectedZ = new Line(new double[] { 20.0, 30.0 }, new double[] { 10.0, 15.0 }, new double[] { 100.0, 200.0 });
        assertEquals(expectedZ, WellKnownText.fromWKT(validator, true, "LINESTRING Z (20.0 10.0 100.0, 30.0 15.0 200.0)"));

        Line expectedM = new Line(new double[] { 20.0, 30.0 }, new double[] { 10.0, 15.0 }, new double[] { 100.0, 200.0 });
        assertEquals(expectedM, WellKnownText.fromWKT(validator, true, "LINESTRING M (20.0 10.0 100.0, 30.0 15.0 200.0)"));
    }

    public void testParseLineZorMWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = GeographyValidator.instance(true);
        List<String> linesWkt = List.of("LINESTRING Z (20.0 10.0, 30.0 15.0)", "LINESTRING M (20.0 10.0, 30.0 15.0)");
        for (String line : linesWkt) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, line));
            assertEquals(ZorMMustIncludeThreeValuesMsg, ex.getMessage());
        }
    }

    @Override
    protected Line mutateInstance(Line instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
