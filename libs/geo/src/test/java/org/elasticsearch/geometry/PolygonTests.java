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
import java.util.Collections;
import java.util.List;

public class PolygonTests extends BaseGeometryTestCase<Polygon> {
    @Override
    protected Polygon createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomPolygon(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals(
            "POLYGON ((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0))",
            WellKnownText.toWKT(new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 })))
        );
        assertEquals(
            new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 })),
            WellKnownText.fromWKT(validator, true, "POLYGON ((3 1, 4 2, 5 3, 3 1))")
        );

        assertEquals(
            "POLYGON ((3.0 1.0 5.0, 4.0 2.0 4.0, 5.0 3.0 3.0, 3.0 1.0 5.0))",
            WellKnownText.toWKT(
                new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 5, 4, 3, 5 }))
            )
        );
        assertEquals(
            new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 5, 4, 3, 5 })),
            WellKnownText.fromWKT(validator, true, "POLYGON ((3 1 5, 4 2 4, 5 3 3, 3 1 5))")
        );

        // Auto closing in coerce mode
        assertEquals(
            new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 })),
            WellKnownText.fromWKT(validator, true, "POLYGON ((3 1, 4 2, 5 3))")
        );
        assertEquals(
            new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 5, 4, 3, 5 })),
            WellKnownText.fromWKT(validator, true, "POLYGON ((3 1 5, 4 2 4, 5 3 3))")
        );
        assertEquals(
            new Polygon(
                new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }),
                Collections.singletonList(new LinearRing(new double[] { 0.5, 2.5, 2.0, 0.5 }, new double[] { 1.5, 1.5, 1.0, 1.5 }))
            ),
            WellKnownText.fromWKT(validator, true, "POLYGON ((3 1, 4 2, 5 3, 3 1), (0.5 1.5, 2.5 1.5, 2.0 1.0))")
        );

        assertEquals("POLYGON EMPTY", WellKnownText.toWKT(Polygon.EMPTY));
        assertEquals(Polygon.EMPTY, WellKnownText.fromWKT(validator, true, "POLYGON EMPTY)"));
    }

    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[] { 3, 4, 3 }, new double[] { 1, 2, 1 }))
        );
        assertEquals("at least 4 polygon points required", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }), null)
        );
        assertEquals("holes must not be null", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> new Polygon(
                new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 5, 4, 3, 5 }),
                Collections.singletonList(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }))
            )
        );
        assertEquals("holes must have the same number of dimensions as the polygon", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> StandardValidator.instance(false)
                .validate(
                    new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 1, 2, 3, 1 }))
                )
        );
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true)
            .validate(new Polygon(new LinearRing(new double[] { 3, 4, 5, 3 }, new double[] { 1, 2, 3, 1 }, new double[] { 1, 2, 3, 1 })));
    }

    public void testWKTValidation() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(GeographyValidator.instance(true), false, "polygon ((3 1 5, 4 2 4, 5 3 3))")
        );
        assertEquals(
            "first and last points of the linear ring must be the same (it must close itself): "
                + "x[0]=3.0 x[2]=5.0 y[0]=1.0 y[2]=3.0 z[0]=5.0 z[2]=3.0",
            ex.getMessage()
        );

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(GeographyValidator.instance(false), randomBoolean(), "polygon ((3 1 5, 4 2 4, 5 3 3, 3 1 5))")
        );
        assertEquals("found Z value [5.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownText.fromWKT(
                GeographyValidator.instance(randomBoolean()),
                false,
                "polygon ((3 1, 4 2, 5 3, 3 1), (0.5 1.5, 2.5 1.5, 2.0 1.0))"
            )
        );
        assertEquals(
            "first and last points of the linear ring must be the same (it must close itself): x[0]=0.5 x[2]=2.0 y[0]=1.5 y[2]=1.0",
            ex.getMessage()
        );
    }

    public void testParsePolygonZorMWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);

        Polygon expectedZ = new Polygon(
            new LinearRing(
                new double[] { 20.0, 30.0, 40.0, 20.0 },
                new double[] { 10.0, 15.0, 10.0, 10.0 },
                new double[] { 100.0, 200.0, 300.0, 100.0 }
            )
        );
        assertEquals(
            expectedZ,
            WellKnownText.fromWKT(validator, true, "POLYGON Z ((20.0 10.0 100.0, 30.0 15.0 200.0, 40.0 10.0 300.0, 20.0 10.0 100.0))")
        );

        Polygon expectedM = new Polygon(
            new LinearRing(
                new double[] { 20.0, 30.0, 40.0, 20.0 },
                new double[] { 10.0, 15.0, 10.0, 10.0 },
                new double[] { 100.0, 200.0, 300.0, 100.0 }
            )
        );
        assertEquals(
            expectedM,
            WellKnownText.fromWKT(validator, true, "POLYGON M ((20.0 10.0 100.0, 30.0 15.0 200.0, 40.0 10.0 300.0, 20.0 10.0 100.0))")
        );

        Polygon expectedZAutoClose = new Polygon(
            new LinearRing(
                new double[] { 20.0, 30.0, 40.0, 20.0 },
                new double[] { 10.0, 15.0, 10.0, 10.0 },
                new double[] { 100.0, 200.0, 300.0, 100.0 }
            )
        );
        assertEquals(
            expectedZAutoClose,
            WellKnownText.fromWKT(validator, true, "POLYGON Z ((20.0 10.0 100.0, 30.0 15.0 200.0, 40.0 10.0 300.0))")
        );
    }

    public void testParsePolygonZorMWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = GeographyValidator.instance(true);
        List<String> polygonsWkt = List.of(
            "POLYGON Z ((20.0 10.0, 30.0 15.0, 40.0 10.0, 20.0 10.0))",
            "POLYGON M ((20.0 10.0, 30.0 15.0, 40.0 10.0, 20.0 10.0))"
        );
        for (String polygon : polygonsWkt) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> WellKnownText.fromWKT(validator, true, polygon)
            );
            assertEquals(ZorMMustIncludeThreeValuesMsg, ex.getMessage());
        }
    }

    @Override
    protected Polygon mutateInstance(Polygon instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
