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

import org.elasticsearch.common.unit.DistanceUnit;

import java.io.IOException;

public class CircleBuilderTests extends AbstractShapeBuilderTestCase<CircleBuilder> {

    @Override
    protected CircleBuilder createTestShapeBuilder() {
        return createRandomShape();
    }

    @Override
    protected CircleBuilder createMutation(CircleBuilder original) throws IOException {
        return mutate(original);
    }

    static CircleBuilder mutate(CircleBuilder original) throws IOException {
        CircleBuilder mutation = copyShape(original);
        double radius = original.radius();
        DistanceUnit unit = original.unit();

        if (randomBoolean()) {
            if (original.center().x > 0.0 || original.center().y > 0.0) {
                mutation.center(new Coordinate(original.center().x/2, original.center().y/2));
            } else {
                // original center was 0.0, 0.0
                mutation.center(randomDouble() + 0.1, randomDouble() + 0.1);
            }
        } else if (randomBoolean()) {
            if (radius > 0) {
                radius = radius/2;
            } else {
                radius = randomDouble() + 0.1;
            }
        } else {
            DistanceUnit newRandom = unit;
            while (newRandom == unit) {
                newRandom = randomFrom(DistanceUnit.values());
            };
            unit = newRandom;
        }
        return mutation.radius(radius, unit);
    }

    static CircleBuilder createRandomShape() {
        CircleBuilder circle = new CircleBuilder();
        if (frequently()) {
            double centerX = randomDoubleBetween(-180, 180, false);
            double centerY = randomDoubleBetween(-90, 90, false);
            circle.center(centerX, centerY);
        }
        if (randomBoolean()) {
            circle.radius(randomDoubleBetween(0.1, 10.0, false), randomFrom(DistanceUnit.values()));
        }
        return circle;
    }
}
