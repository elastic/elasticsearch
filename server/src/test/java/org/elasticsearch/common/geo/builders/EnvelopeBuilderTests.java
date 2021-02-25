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
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;

public class EnvelopeBuilderTests extends AbstractShapeBuilderTestCase<EnvelopeBuilder> {

    public void testInvalidConstructorArgs() {
        NullPointerException e;
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(null, new Coordinate(1.0, -1.0)));
        assertEquals("topLeft of envelope cannot be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(new Coordinate(1.0, -1.0), null));
        assertEquals("bottomRight of envelope cannot be null", e.getMessage());
    }

    @Override
    protected EnvelopeBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected EnvelopeBuilder createMutation(EnvelopeBuilder original) throws IOException {
        return mutate(original);
    }

    static EnvelopeBuilder mutate(EnvelopeBuilder original) throws IOException {
        EnvelopeBuilder mutation = (EnvelopeBuilder) copyShape(original);
        // move one corner to the middle of original
        switch (randomIntBetween(0, 3)) {
        case 0:
            mutation = new EnvelopeBuilder(
                    new Coordinate(randomDoubleBetween(-180.0, original.bottomRight().x, true), original.topLeft().y),
                    original.bottomRight());
            break;
        case 1:
            mutation = new EnvelopeBuilder(new Coordinate(original.topLeft().x, randomDoubleBetween(original.bottomRight().y, 90.0, true)),
                    original.bottomRight());
            break;
        case 2:
            mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(randomDoubleBetween(original.topLeft().x, 180.0, true), original.bottomRight().y));
            break;
        case 3:
            mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(original.bottomRight().x, randomDoubleBetween(-90.0, original.topLeft().y, true)));
            break;
        }
        return mutation;
    }

    static EnvelopeBuilder createRandomShape() {
        Rectangle box = RandomShapeGenerator.xRandomRectangle(random(), RandomShapeGenerator.xRandomPoint(random()));
        EnvelopeBuilder envelope = new EnvelopeBuilder(new Coordinate(box.getMinX(), box.getMaxY()),
                new Coordinate(box.getMaxX(), box.getMinY()));
        return envelope;
    }
}
