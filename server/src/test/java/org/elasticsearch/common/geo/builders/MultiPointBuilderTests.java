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

import org.locationtech.jts.geom.Coordinate;

import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;

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
        MultiPointBuilder mutation = (MultiPointBuilder) copyShape(original);
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
            coordinates = new Coordinate[]{new Coordinate(1.0, 1.0)};
        }
        return MultiPointBuilder.class.cast(mutation.coordinates(coordinates));
    }

    static MultiPointBuilder createRandomShape() {
        return (MultiPointBuilder) RandomShapeGenerator.createShape(random(), ShapeType.MULTIPOINT);
    }
}
