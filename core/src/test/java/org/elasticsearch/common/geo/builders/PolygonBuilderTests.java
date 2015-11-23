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

import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;

import java.io.IOException;

public class PolygonBuilderTests extends AbstractShapeBuilderTestCase<PolygonBuilder> {

    static final PolygonBuilderTests PROTOTYPE = new PolygonBuilderTests();

    @Override
    protected PolygonBuilder createTestShapeBuilder() {
        PolygonBuilder pgb = (PolygonBuilder) RandomShapeGenerator.createShape(getRandom(), ShapeType.POLYGON);
        pgb.orientation = randomFrom(Orientation.values());
        // NORELEASE translated might have been changed by createShape, but won't survive xContent->Parse roundtrip
        pgb.shell().translated(false);
        return pgb;
    }

    @Override
    protected PolygonBuilder mutate(PolygonBuilder original) throws IOException {
        PolygonBuilder mutation = copyShape(original);
        return mutatePolygonBuilder(mutation);
    }

    static PolygonBuilder mutatePolygonBuilder(PolygonBuilder pb) {
        if (randomBoolean()) {
            // toggle orientation
            pb.orientation = (pb.orientation == Orientation.LEFT ? Orientation.RIGHT : Orientation.LEFT);
        } else {
            // change either point in shell or in random hole
            LineStringBuilder lineToChange;
            if (randomBoolean() || pb.holes().size() == 0) {
                lineToChange = pb.shell();
            } else {
                lineToChange = randomFrom(pb.holes());
            }
            Coordinate coordinate = randomFrom(lineToChange.coordinates(false));
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
        return pb;
    }
}
