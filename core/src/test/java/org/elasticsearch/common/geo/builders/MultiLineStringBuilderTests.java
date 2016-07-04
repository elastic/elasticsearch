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

import com.vividsolutions.jts.geom.Coordinate;
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
        MultiLineStringBuilder mutation = (MultiLineStringBuilder) copyShape(original);
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
        return new MultiLineStringBuilder();
    }
}
