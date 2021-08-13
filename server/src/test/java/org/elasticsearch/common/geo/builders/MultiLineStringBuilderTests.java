/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo.builders;

import org.locationtech.jts.geom.Coordinate;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;

import java.io.IOException;

public class MultiLineStringBuilderTests extends AbstractShapeBuilderTestCase<MultiLineStringBuilder> {

    @Override
    protected MultiLineStringBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected MultiLineStringBuilder createMutation(MultiLineStringBuilder original) throws IOException {
        return mutate(original);
    }

    static MultiLineStringBuilder mutate(MultiLineStringBuilder original) throws IOException {
        MultiLineStringBuilder mutation = copyShape(original);
        Coordinate[][] coordinates = mutation.coordinates();
        if (coordinates.length > 0) {
            int lineToChange = randomInt(coordinates.length - 1);
            for (int i = 0; i < coordinates.length; i++) {
                Coordinate[] line = coordinates[i];
                if (i == lineToChange) {
                    Coordinate coordinate = randomFrom(line);
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
            }
        } else {
            mutation.linestring((LineStringBuilder) RandomShapeGenerator.createShape(random(), ShapeType.LINESTRING));
        }
        return mutation;
    }

    static MultiLineStringBuilder createRandomShape() {
        return MultiLineStringBuilder.class.cast(RandomShapeGenerator.createShape(random(), ShapeType.MULTILINESTRING));
    }
}
