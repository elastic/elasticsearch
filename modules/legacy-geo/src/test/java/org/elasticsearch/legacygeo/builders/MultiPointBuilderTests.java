/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.legacygeo.test.RandomShapeGenerator;
import org.elasticsearch.legacygeo.test.RandomShapeGenerator.ShapeType;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.util.List;

public class MultiPointBuilderTests extends AbstractShapeBuilderTestCase<MultiPointBuilder> {

    public void testInvalidBuilderException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MultiPointBuilder((List<Coordinate>) null));
        assertEquals("cannot create point collection with empty set of points", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new MultiPointBuilder(new CoordinatesBuilder().build()));
        assertEquals("cannot create point collection with empty set of points", e.getMessage());

        // one point is minimum
        new MultiPointBuilder(new CoordinatesBuilder().coordinate(0.0, 0.0).build());
    }

    @Override
    protected MultiPointBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected MultiPointBuilder createMutation(MultiPointBuilder original) throws IOException {
        return mutate(original);
    }

    static MultiPointBuilder mutate(MultiPointBuilder original) throws IOException {
        MultiPointBuilder mutation = copyShape(original);
        Coordinate[] coordinates = original.coordinates(false);
        if (coordinates.length > 0) {
            Coordinate coordinate = randomFrom(coordinates);
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
        } else {
            coordinates = new Coordinate[] { new Coordinate(1.0, 1.0) };
        }
        return MultiPointBuilder.class.cast(mutation.coordinates(coordinates));
    }

    static MultiPointBuilder createRandomShape() {
        return (MultiPointBuilder) RandomShapeGenerator.createShape(random(), ShapeType.MULTIPOINT);
    }
}
