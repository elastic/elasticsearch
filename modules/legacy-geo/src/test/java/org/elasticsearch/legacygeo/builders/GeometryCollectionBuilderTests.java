/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.legacygeo.test.RandomShapeGenerator;

import java.io.IOException;

public class GeometryCollectionBuilderTests extends AbstractShapeBuilderTestCase<GeometryCollectionBuilder> {

    @Override
    protected GeometryCollectionBuilder createTestShapeBuilder() {
        GeometryCollectionBuilder geometryCollection = new GeometryCollectionBuilder();
        int shapes = randomIntBetween(0, 8);
        for (int i = 0; i < shapes; i++) {
            switch (randomIntBetween(0, 7)) {
                case 0 -> geometryCollection.shape(PointBuilderTests.createRandomShape());
                case 1 -> geometryCollection.shape(CircleBuilderTests.createRandomShape());
                case 2 -> geometryCollection.shape(EnvelopeBuilderTests.createRandomShape());
                case 3 -> geometryCollection.shape(LineStringBuilderTests.createRandomShape());
                case 4 -> geometryCollection.shape(MultiLineStringBuilderTests.createRandomShape());
                case 5 -> geometryCollection.shape(MultiPolygonBuilderTests.createRandomShape());
                case 6 -> geometryCollection.shape(MultiPointBuilderTests.createRandomShape());
                case 7 -> geometryCollection.shape(PolygonBuilderTests.createRandomShape());
            }
        }
        return geometryCollection;
    }

    @Override
    protected GeometryCollectionBuilder createMutation(GeometryCollectionBuilder original) throws IOException {
        return mutate(original);
    }

    static GeometryCollectionBuilder mutate(GeometryCollectionBuilder original) throws IOException {
        GeometryCollectionBuilder mutation = copyShape(original);
        if (mutation.shapes.size() > 0) {
            int shapePosition = randomIntBetween(0, mutation.shapes.size() - 1);
            ShapeBuilder<?, ?, ?> shapeToChange = mutation.shapes.get(shapePosition);
            mutation.shapes.set(shapePosition, switch (shapeToChange.type()) {
                case POINT -> PointBuilderTests.mutate((PointBuilder) shapeToChange);
                case CIRCLE -> CircleBuilderTests.mutate((CircleBuilder) shapeToChange);
                case ENVELOPE -> EnvelopeBuilderTests.mutate((EnvelopeBuilder) shapeToChange);
                case LINESTRING -> LineStringBuilderTests.mutate((LineStringBuilder) shapeToChange);
                case MULTILINESTRING -> MultiLineStringBuilderTests.mutate((MultiLineStringBuilder) shapeToChange);
                case MULTIPOLYGON -> MultiPolygonBuilderTests.mutate((MultiPolygonBuilder) shapeToChange);
                case MULTIPOINT -> MultiPointBuilderTests.mutate((MultiPointBuilder) shapeToChange);
                case POLYGON -> PolygonBuilderTests.mutate((PolygonBuilder) shapeToChange);
                case GEOMETRYCOLLECTION -> throw new UnsupportedOperationException(
                    "GeometryCollection should not be nested inside each other"
                );
            });
        } else {
            mutation.shape(RandomShapeGenerator.createShape(random()));
        }
        return mutation;
    }
}
