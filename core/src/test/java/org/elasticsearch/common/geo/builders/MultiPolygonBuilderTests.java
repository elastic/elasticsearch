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

import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
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
                mutation.polygon((PolygonBuilder) copyShape(pb));
            }
        } else {
            mutation = (MultiPolygonBuilder) copyShape(original);
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
