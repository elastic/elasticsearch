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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiPointTests extends BaseGeometryTestCase<MultiPoint> {

    @Override
    protected MultiPoint createTestInstance(boolean hasAlt) {
        int size = randomIntBetween(1, 10);
        List<Point> arr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            arr.add(GeometryTestUtils.randomPoint(hasAlt));
        }
        return new MultiPoint(arr);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals("MULTIPOINT (2.0 1.0)", WellKnownText.toWKT(new MultiPoint(Collections.singletonList(new Point(2, 1)))));
        assertEquals(
            new MultiPoint(Collections.singletonList(new Point(2, 1))),
            WellKnownText.fromWKT(validator, true, "MULTIPOINT (2 1)")
        );

        assertEquals("MULTIPOINT (2.0 1.0, 3.0 4.0)", WellKnownText.toWKT(new MultiPoint(Arrays.asList(new Point(2, 1), new Point(3, 4)))));
        assertEquals(
            new MultiPoint(Arrays.asList(new Point(2, 1), new Point(3, 4))),
            WellKnownText.fromWKT(validator, true, "MULTIPOINT (2 1, 3 4)")
        );

        assertEquals(
            "MULTIPOINT (2.0 1.0 10.0, 3.0 4.0 20.0)",
            WellKnownText.toWKT(new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(3, 4, 20))))
        );
        assertEquals(
            new MultiPoint(Arrays.asList(new Point(2, 1, 10), new Point(3, 4, 20))),
            WellKnownText.fromWKT(validator, true, "MULTIPOINT (2 1 10, 3 4 20)")
        );

        assertEquals("MULTIPOINT EMPTY", WellKnownText.toWKT(MultiPoint.EMPTY));
        assertEquals(MultiPoint.EMPTY, WellKnownText.fromWKT(validator, true, "MULTIPOINT EMPTY)"));
    }

    public void testValidation() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> StandardValidator.instance(false).validate(new MultiPoint(Collections.singletonList(new Point(2, 1, 3))))
        );
        assertEquals("found Z value [3.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(new MultiPoint(Collections.singletonList(new Point(2, 1, 3))));
    }

    public void testParseMultiPointWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        MultiPoint expectedZ = new MultiPoint(Arrays.asList(new Point(10, 20, 30), new Point(40, 50, 60)));
        MultiPoint expectedM = new MultiPoint(Arrays.asList(new Point(10, 20, 30), new Point(40, 50, 60)));

        assertEquals(expectedZ, WellKnownText.fromWKT(validator, true, "MULTIPOINT Z (10 20 30, 40 50 60)"));
        assertEquals(expectedM, WellKnownText.fromWKT(validator, true, "MULTIPOINT M (10 20 30, 40 50 60)"));
    }

    public void testParseMultiPointWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = GeographyValidator.instance(true);
        List<String> multiPointsWkt = List.of("MULTIPOINT Z (10 20, 40 50)", "MULTIPOINT M (10 20, 40 50)");
        for (String multiPoint : multiPointsWkt) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> WellKnownText.fromWKT(validator, true, multiPoint)
            );
            assertEquals(ZorMMustIncludeThreeValuesMsg, ex.getMessage());
        }
    }

    @Override
    protected MultiPoint mutateInstance(MultiPoint instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
