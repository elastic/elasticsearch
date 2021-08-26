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
}
