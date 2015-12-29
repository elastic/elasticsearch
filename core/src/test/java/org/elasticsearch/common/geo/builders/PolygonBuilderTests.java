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

    @Override
    protected PolygonBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected PolygonBuilder createMutation(PolygonBuilder original) throws IOException {
        return mutate(original);
    }

    static PolygonBuilder mutate(PolygonBuilder original) throws IOException {
        PolygonBuilder mutation = (PolygonBuilder) copyShape(original);
        return mutatePolygonBuilder(mutation);
    }

    static PolygonBuilder mutatePolygonBuilder(PolygonBuilder pb) {
        if (randomBoolean()) {
            pb = polyWithOposingOrientation(pb);
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

    /**
     * Takes an input polygon and returns an identical one, only with opposing orientation setting.
     * This is done so we don't have to expose a setter for orientation in the actual class
     */
    private static PolygonBuilder polyWithOposingOrientation(PolygonBuilder pb) {
        PolygonBuilder mutation = new PolygonBuilder(pb.orientation() == Orientation.LEFT ? Orientation.RIGHT : Orientation.LEFT);
        mutation.points(pb.shell().coordinates(false));
        for (LineStringBuilder hole : pb.holes()) {
            mutation.hole(hole);
        }
        return mutation;
    }

    static PolygonBuilder createRandomShape() {
        PolygonBuilder pgb = (PolygonBuilder) RandomShapeGenerator.createShape(getRandom(), ShapeType.POLYGON);
        if (randomBoolean()) {
            pgb = polyWithOposingOrientation(pgb);
        }
        return pgb;
    }
}
