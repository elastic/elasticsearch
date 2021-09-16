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

public class LineStringBuilderTests extends AbstractShapeBuilderTestCase<LineStringBuilder> {

    public void testInvalidConstructorArgs() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new LineStringBuilder((List<Coordinate>) null));
        assertEquals("cannot create point collection with empty set of points", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new LineStringBuilder(new CoordinatesBuilder()));
        assertEquals("cannot create point collection with empty set of points", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new LineStringBuilder(new CoordinatesBuilder().coordinate(0.0, 0.0)));
        assertEquals("invalid number of points in LineString (found [1] - must be >= 2)", e.getMessage());
    }

    @Override
    protected LineStringBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected LineStringBuilder createMutation(LineStringBuilder original) throws IOException {
        return mutate(original);
    }

    static LineStringBuilder mutate(LineStringBuilder original) throws IOException {
        LineStringBuilder mutation = copyShape(original);
        Coordinate[] coordinates = original.coordinates(false);
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
        return LineStringBuilder.class.cast(mutation.coordinates(coordinates));
    }

    static LineStringBuilder createRandomShape() {
        LineStringBuilder lsb = (LineStringBuilder) RandomShapeGenerator.createShape(random(), ShapeType.LINESTRING);
        if (randomBoolean()) {
            lsb.close();
        }
        return lsb;
    }
}
