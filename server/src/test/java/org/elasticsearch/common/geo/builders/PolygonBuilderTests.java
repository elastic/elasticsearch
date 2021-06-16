/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.locationtech.jts.geom.Coordinate;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PolygonBuilderTests extends AbstractShapeBuilderTestCase<PolygonBuilder> {

    @Override
    protected PolygonBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected PolygonBuilder createMutation(PolygonBuilder original) throws IOException {
        return mutate(original);
    }

    static PolygonBuilder mutate(PolygonBuilder original) throws IOException {
        PolygonBuilder mutation = (PolygonBuilder) copyShape(original);
        return mutatePolygonBuilder(mutation);
    }

    static PolygonBuilder mutatePolygonBuilder(PolygonBuilder pb) {
        if (randomBoolean()) {
            pb = polyWithOposingOrientation(pb);
        } else {
            // change either point in shell or in random hole
            LineStringBuilder lineToChange;
            if (randomBoolean() || pb.holes().size() == 0) {
                lineToChange = pb.shell();
            } else {
                lineToChange = randomFrom(pb.holes());
            }
            Coordinate coordinate = randomFrom(lineToChange.coordinates(false));
            if (randomBoolean()) {
                if (coordinate.x != 0.0) {
                    coordinate.x = coordinate.x / 2;
                } else {
                    coordinate.x = randomDoubleBetween(-180.0, 180.0, true);
                }
            } else {
                if (coordinate.y != 0.0) {
                    coordinate.y = coordinate.y / 2;
                } else {
                    coordinate.y = randomDoubleBetween(-90.0, 90.0, true);
                }
            }
        }
        return pb;
    }

    /**
     * Takes an input polygon and returns an identical one, only with opposing orientation setting.
     * This is done so we don't have to expose a setter for orientation in the actual class
     */
    private static PolygonBuilder polyWithOposingOrientation(PolygonBuilder pb) {
        PolygonBuilder mutation = new PolygonBuilder(pb.shell(),
                pb.orientation() == Orientation.LEFT ? Orientation.RIGHT : Orientation.LEFT);
        for (LineStringBuilder hole : pb.holes()) {
            mutation.hole(hole);
        }
        return mutation;
    }

    static PolygonBuilder createRandomShape() {
        PolygonBuilder pgb = (PolygonBuilder) RandomShapeGenerator.createShape(random(), ShapeType.POLYGON);
        if (randomBoolean()) {
            pgb = polyWithOposingOrientation(pgb);
        }
        return pgb;
    }

    public void testCoerceShell() {
        try{
            new PolygonBuilder(new LineStringBuilder(new CoordinatesBuilder().coordinate(0.0, 0.0)
                    .coordinate(1.0, 0.0).coordinate(1.0, 1.0).build()), Orientation.RIGHT);
            fail("should raise validation exception");
        } catch (IllegalArgumentException e) {
            assertEquals("invalid number of points in LinearRing (found [3] - must be >= 4)", e.getMessage());
        }

        PolygonBuilder pb = new PolygonBuilder(new LineStringBuilder(new CoordinatesBuilder().coordinate(0.0, 0.0)
                .coordinate(1.0, 0.0).coordinate(1.0, 1.0).build()), Orientation.RIGHT, true);
        assertThat("Shell should have been closed via coerce", pb.shell().coordinates(false).length, equalTo(4));
    }

    public void testCoerceHole() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder().coordinate(0.0, 0.0)
                .coordinate(2.0, 0.0).coordinate(2.0, 2.0).coordinate(0.0, 0.0));
        try{
            pb.hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(0.0,0.0).coordinate(1.0,0.0).coordinate(1.0,1.0).build()));
            fail("should raise validation exception");
        } catch (IllegalArgumentException e) {
            assertEquals("invalid number of points in LinearRing (found [3] - must be >= 4)", e.getMessage());
        }

        pb.hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(0.0,0.0).coordinate(1.0,0.0).coordinate(1.0,1.0).build()), true);
        assertThat("hole should have been closed via coerce", pb.holes().get(0).coordinates(false).length, equalTo(4));
    }

    public void testHoleThatIsSouthOfPolygon() {
        InvalidShapeException e = expectThrows(InvalidShapeException.class, () -> {
            PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder().coordinate(4, 3).coordinate(3, 2).coordinate(3, 3).close());
            pb.hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(4, 2).coordinate(3, 1).coordinate(4, 1).close()));
            pb.buildS4J();
        });

        assertEquals("Hole lies outside shell at or near point (4.0, 1.0, NaN)", e.getMessage());
    }

    public void testHoleThatIsNorthOfPolygon() {
        InvalidShapeException e = expectThrows(InvalidShapeException.class, () -> {
            PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder().coordinate(3, 2).coordinate(4, 1).coordinate(3, 1).close());
            pb.hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(3, 3).coordinate(4, 2).coordinate(4, 3).close()));
            pb.buildS4J();
        });

        assertEquals("Hole lies outside shell at or near point (4.0, 3.0, NaN)", e.getMessage());
    }

    public void testWidePolygonWithConfusingOrientation() {
        // A valid polygon that is oriented correctly (anticlockwise) but which
        // confounds a naive algorithm for determining its orientation leading
        // ES to believe that it crosses the dateline and "fixing" it in a way
        // that self-intersects.

        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(10, -20).coordinate(100, 0).coordinate(-100, 0).coordinate(20, -45).coordinate(40, -60).close());
        pb.buildS4J(); // Should not throw an exception
    }

    public void testPolygonWithUndefinedOrientationDueToCollinearPoints() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(0.0, 0.0).coordinate(1.0, 1.0).coordinate(-1.0, -1.0).close());
        InvalidShapeException e = expectThrows(InvalidShapeException.class, pb::buildS4J);
        assertEquals("Cannot determine orientation: signed area equal to 0", e.getMessage());
    }

    public void testCrossingDateline() {
        PolygonBuilder pb = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(170, -10).coordinate(-170, -10).coordinate(-170, 10).coordinate(170, 10).coordinate(170, -10));
        JtsGeometry geometry = pb.buildS4J();

        assertTrue(geometry.getGeom() instanceof org.locationtech.jts.geom.MultiPolygon);
        pb = new PolygonBuilder(new CoordinatesBuilder()
            .coordinate(180, -10).coordinate(-170, -5).coordinate(-170, 15).coordinate(170, -15).coordinate(180, -10));
        geometry = pb.buildS4J();
        assertTrue(geometry.getGeom() instanceof org.locationtech.jts.geom.MultiPolygon);
    }
}
