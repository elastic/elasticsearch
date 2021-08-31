/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.locationtech.spatial4j.exception.InvalidShapeException;

import java.io.IOException;

public class MultiPolygonBuilderTests extends AbstractShapeBuilderTestCase<MultiPolygonBuilder> {

    @Override
    protected MultiPolygonBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected MultiPolygonBuilder createMutation(MultiPolygonBuilder original) throws IOException {
        return mutate(original);
    }

    static MultiPolygonBuilder mutate(MultiPolygonBuilder original) throws IOException {
        MultiPolygonBuilder mutation;
        if (randomBoolean()) {
            mutation = new MultiPolygonBuilder(original.orientation() == Orientation.LEFT ? Orientation.RIGHT : Orientation.LEFT);
            for (PolygonBuilder pb : original.polygons()) {
                mutation.polygon(copyShape(pb));
            }
        } else {
            mutation = copyShape(original);
            if (mutation.polygons().size() > 0) {
                int polyToChange = randomInt(mutation.polygons().size() - 1);
                mutation.polygons().set(polyToChange, PolygonBuilderTests.mutatePolygonBuilder(mutation.polygons().get(polyToChange)));
            } else {
                mutation.polygon((PolygonBuilder) RandomShapeGenerator.createShape(random(), ShapeType.POLYGON));
            }
        }
        return mutation;
    }

    static MultiPolygonBuilder createRandomShape() {
        MultiPolygonBuilder mpb = new MultiPolygonBuilder(randomFrom(Orientation.values()));
        int polys = randomIntBetween(0, 10);
        for (int i = 0; i < polys; i++) {
            PolygonBuilder pgb = (PolygonBuilder) RandomShapeGenerator.createShape(random(), ShapeType.POLYGON);
            mpb.polygon(pgb);
        }
        return mpb;
    }

    public void testInvalidPolygonBuilders() {
        try {
            // self intersection polygon
            new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10)
                .coordinate(10, 10)
                .coordinate(-10, 10)
                .coordinate(10, -10)
                .close())
                .buildS4J();
            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        // polygon with hole
        new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
            .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5)
                .coordinate(5, -5).close()))
            .buildS4J();
        try {
            // polygon with overlapping hole
            new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder()
                    .coordinate(-5, -5).coordinate(-5, 11).coordinate(5, 11).coordinate(5, -5).close()))
                .buildS4J();

            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        try {
            // polygon with intersection holes
            new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5)
                    .coordinate(5, -5).close()))
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -6).coordinate(5, -6).coordinate(5, -4)
                    .coordinate(-5, -4).close()))
                .buildS4J();
            fail("Intersection of holes not detected");
        } catch (InvalidShapeException e) {
        }

        try {
            // Common line in polygon
            new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10)
                .coordinate(-10, 10)
                .coordinate(-5, 10)
                .coordinate(-5, -5)
                .coordinate(-5, 20)
                .coordinate(10, 20)
                .coordinate(10, -10)
                .close())
                .buildS4J();
            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        // Multipolygon: polygon with hole and polygon within the whole
        new MultiPolygonBuilder()
            .polygon(new PolygonBuilder(
                new CoordinatesBuilder().coordinate(-10, -10)
                    .coordinate(-10, 10)
                    .coordinate(10, 10)
                    .coordinate(10, -10).close())
                .hole(new LineStringBuilder(
                    new CoordinatesBuilder().coordinate(-5, -5)
                        .coordinate(-5, 5)
                        .coordinate(5, 5)
                        .coordinate(5, -5).close())))
            .polygon(new PolygonBuilder(
                new CoordinatesBuilder()
                    .coordinate(-4, -4)
                    .coordinate(-4, 4)
                    .coordinate(4, 4)
                    .coordinate(4, -4).close()))
            .buildS4J();
    }
}
