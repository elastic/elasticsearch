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
