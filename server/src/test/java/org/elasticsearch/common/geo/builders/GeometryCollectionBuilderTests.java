/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;

public class GeometryCollectionBuilderTests extends AbstractShapeBuilderTestCase<GeometryCollectionBuilder> {

    @Override
    protected GeometryCollectionBuilder createTestShapeBuilder() {
        GeometryCollectionBuilder geometryCollection = new GeometryCollectionBuilder();
        int shapes = randomIntBetween(0, 8);
        for (int i = 0; i < shapes; i++) {
            switch (randomIntBetween(0, 7)) {
            case 0:
                geometryCollection.shape(PointBuilderTests.createRandomShape());
                break;
            case 1:
                geometryCollection.shape(CircleBuilderTests.createRandomShape());
                break;
            case 2:
                geometryCollection.shape(EnvelopeBuilderTests.createRandomShape());
                break;
            case 3:
                geometryCollection.shape(LineStringBuilderTests.createRandomShape());
                break;
            case 4:
                geometryCollection.shape(MultiLineStringBuilderTests.createRandomShape());
                break;
            case 5:
                geometryCollection.shape(MultiPolygonBuilderTests.createRandomShape());
                break;
            case 6:
                geometryCollection.shape(MultiPointBuilderTests.createRandomShape());
                break;
            case 7:
                geometryCollection.shape(PolygonBuilderTests.createRandomShape());
                break;
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
            switch (shapeToChange.type()) {
            case POINT:
                shapeToChange = PointBuilderTests.mutate((PointBuilder) shapeToChange);
                break;
            case CIRCLE:
                shapeToChange = CircleBuilderTests.mutate((CircleBuilder) shapeToChange);
                break;
            case ENVELOPE:
                shapeToChange = EnvelopeBuilderTests.mutate((EnvelopeBuilder) shapeToChange);
                break;
            case LINESTRING:
                shapeToChange = LineStringBuilderTests.mutate((LineStringBuilder) shapeToChange);
                break;
            case MULTILINESTRING:
                shapeToChange = MultiLineStringBuilderTests.mutate((MultiLineStringBuilder) shapeToChange);
                break;
            case MULTIPOLYGON:
                shapeToChange = MultiPolygonBuilderTests.mutate((MultiPolygonBuilder) shapeToChange);
                break;
            case MULTIPOINT:
                shapeToChange = MultiPointBuilderTests.mutate((MultiPointBuilder) shapeToChange);
                break;
            case POLYGON:
                shapeToChange = PolygonBuilderTests.mutate((PolygonBuilder) shapeToChange);
                break;
            case GEOMETRYCOLLECTION:
                throw new UnsupportedOperationException("GeometryCollection should not be nested inside each other");
            }
            mutation.shapes.set(shapePosition, shapeToChange);
        } else {
            mutation.shape(RandomShapeGenerator.createShape(random()));
        }
        return mutation;
    }
}
