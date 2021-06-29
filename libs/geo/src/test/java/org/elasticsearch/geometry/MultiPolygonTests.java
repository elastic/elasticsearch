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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiPolygonTests extends BaseGeometryTestCase<MultiPolygon> {

    @Override
    protected MultiPolygon createTestInstance(boolean hasAlt) {
        int size = randomIntBetween(1, 10);
        List<Polygon> arr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            arr.add(GeometryTestUtils.randomPolygon(hasAlt));
        }
        return new MultiPolygon(arr);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals("MULTIPOLYGON (((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0)))",
            WellKnownText.toWKT(new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}))))));
        assertEquals(new MultiPolygon(Collections.singletonList(
            new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1})))),
            WellKnownText.fromWKT(validator, true, "MULTIPOLYGON (((3.0 1.0, 4.0 2.0, 5.0 3.0, 3.0 1.0)))"));

        assertEquals("MULTIPOLYGON EMPTY", WellKnownText.toWKT(MultiPolygon.EMPTY));
        assertEquals(MultiPolygon.EMPTY, WellKnownText.fromWKT(validator, true, "MULTIPOLYGON EMPTY)"));
    }

    public void testValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> StandardValidator.instance(false).validate(
            new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1}))
            ))));
        assertEquals("found Z value [1.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(
            new MultiPolygon(Collections.singletonList(
                new Polygon(new LinearRing(new double[]{3, 4, 5, 3}, new double[]{1, 2, 3, 1}, new double[]{1, 2, 3, 1})))));
    }
}
