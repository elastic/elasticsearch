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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class GeometryCollectionTests extends BaseGeometryTestCase<GeometryCollection<Geometry>> {
    @Override
    protected GeometryCollection<Geometry> createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomGeometryCollection(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);
        assertEquals(
            "GEOMETRYCOLLECTION (POINT (20.0 10.0),POINT EMPTY)",
            WellKnownText.toWKT(new GeometryCollection<Geometry>(Arrays.asList(new Point(20, 10), Point.EMPTY)))
        );

        assertEquals(
            new GeometryCollection<Geometry>(Arrays.asList(new Point(20, 10), Point.EMPTY)),
            WellKnownText.fromWKT(validator, true, "GEOMETRYCOLLECTION (POINT (20.0 10.0),POINT EMPTY)")
        );

        assertEquals("GEOMETRYCOLLECTION EMPTY", WellKnownText.toWKT(GeometryCollection.EMPTY));
        assertEquals(GeometryCollection.EMPTY, WellKnownText.fromWKT(validator, true, "GEOMETRYCOLLECTION EMPTY)"));
    }

    @SuppressWarnings("ConstantConditions")
    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(Collections.emptyList()));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(null));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> new GeometryCollection<>(Arrays.asList(new Point(20, 10), new Point(20, 10, 30)))
        );
        assertEquals("all elements of the collection should have the same number of dimension", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> StandardValidator.instance(false)
                .validate(new GeometryCollection<Geometry>(Collections.singletonList(new Point(20, 10, 30))))
        );
        assertEquals("found Z value [30.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        StandardValidator.instance(true).validate(new GeometryCollection<Geometry>(Collections.singletonList(new Point(20, 10, 30))));
    }

    public void testDeeplyNestedCollection() throws IOException, ParseException {
        String wkt = makeDeeplyNestedGeometryCollectionWKT(WellKnownText.MAX_NESTED_DEPTH);
        Geometry parsed = WellKnownText.fromWKT(GeographyValidator.instance(true), true, wkt);
        assertEquals(WellKnownText.MAX_NESTED_DEPTH, countNestedGeometryCollections((GeometryCollection<?>) parsed));
    }

    public void testTooDeeplyNestedCollection() {
        String wkt = makeDeeplyNestedGeometryCollectionWKT(WellKnownText.MAX_NESTED_DEPTH + 1);
        ParseException ex = expectThrows(ParseException.class, () -> WellKnownText.fromWKT(GeographyValidator.instance(true), true, wkt));
        assertThat(ex.getMessage(), containsString("maximum nested depth of " + WellKnownText.MAX_NESTED_DEPTH));
    }

    private String makeDeeplyNestedGeometryCollectionWKT(int depth) {
        return "GEOMETRYCOLLECTION (".repeat(depth) + "POINT (20.0 10.0)" + ")".repeat(depth);
    }

    private int countNestedGeometryCollections(GeometryCollection<?> geometry) {
        int count = 1;
        while (geometry.get(0) instanceof GeometryCollection<?> g) {
            count += 1;
            geometry = g;
        }
        return count;
    }

    public void testParseGeometryCollectionZorMWithThreeCoordinates() throws IOException, ParseException {
        GeometryValidator validator = GeographyValidator.instance(true);

        GeometryCollection<Geometry> expected = new GeometryCollection<>(
            Arrays.asList(
                new Point(20.0, 10.0, 100.0),
                new Line(new double[] { 10.0, 20.0 }, new double[] { 5.0, 15.0 }, new double[] { 50.0, 150.0 })
            )
        );

        String point = "(POINT Z (20.0 10.0 100.0)";
        String lineString = "LINESTRING M (10.0 5.0 50.0, 20.0 15.0 150.0)";
        assertEquals(expected, WellKnownText.fromWKT(validator, true, "GEOMETRYCOLLECTION Z " + point + ", " + lineString + ")"));

        assertEquals(expected, WellKnownText.fromWKT(validator, true, "GEOMETRYCOLLECTION M " + point + ", " + lineString + ")"));
    }

    public void testParseGeometryCollectionZorMWithTwoCoordinatesThrowsException() {
        GeometryValidator validator = GeographyValidator.instance(true);
        List<String> gcWkt = List.of(
            "GEOMETRYCOLLECTION Z (POINT (20.0 10.0), LINESTRING (10.0 5.0, 20.0 15.0))",
            "GEOMETRYCOLLECTION M (POINT (20.0 10.0), LINESTRING (10.0 5.0, 20.0 15.0))"
        );
        for (String gc : gcWkt) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(validator, true, gc));
            assertEquals(ZorMMustIncludeThreeValuesMsg, ex.getMessage());
        }
    }

    @Override
    protected GeometryCollection<Geometry> mutateInstance(GeometryCollection<Geometry> instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
