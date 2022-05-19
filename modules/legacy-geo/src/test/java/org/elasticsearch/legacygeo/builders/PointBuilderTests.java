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

public class PointBuilderTests extends AbstractShapeBuilderTestCase<PointBuilder> {

    @Override
    protected PointBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected PointBuilder createMutation(PointBuilder original) throws IOException {
        return mutate(original);
    }

    static PointBuilder mutate(PointBuilder original) {
        return new PointBuilder().coordinate(new Coordinate(original.longitude() / 2, original.latitude() / 2));
    }

    static PointBuilder createRandomShape() {
        return (PointBuilder) RandomShapeGenerator.createShape(random(), ShapeType.POINT);
    }

}
