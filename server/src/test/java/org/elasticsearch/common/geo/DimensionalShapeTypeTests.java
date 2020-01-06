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

package org.elasticsearch.common.geo;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DimensionalShapeTypeTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(DimensionalShapeType.values().length, equalTo(9));
        assertThat(DimensionalShapeType.POINT.ordinal(), equalTo(0));
        assertThat(DimensionalShapeType.MULTIPOINT.ordinal(), equalTo(1));
        assertThat(DimensionalShapeType.LINESTRING.ordinal(), equalTo(2));
        assertThat(DimensionalShapeType.MULTILINESTRING.ordinal(), equalTo(3));
        assertThat(DimensionalShapeType.POLYGON.ordinal(), equalTo(4));
        assertThat(DimensionalShapeType.MULTIPOLYGON.ordinal(), equalTo(5));
        assertThat(DimensionalShapeType.GEOMETRYCOLLECTION_POINTS.ordinal(), equalTo(6));
        assertThat(DimensionalShapeType.GEOMETRYCOLLECTION_LINES.ordinal(), equalTo(7));
        assertThat(DimensionalShapeType.GEOMETRYCOLLECTION_POLYGONS.ordinal(), equalTo(8));
    }

    public void testSerialization() {
        for (DimensionalShapeType type : DimensionalShapeType.values()) {
            ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            type.writeTo(out);
            ByteArrayDataInput input = new ByteArrayDataInput(out.toArrayCopy());
            assertThat(DimensionalShapeType.readFrom(input), equalTo(type));
        }
    }
}
