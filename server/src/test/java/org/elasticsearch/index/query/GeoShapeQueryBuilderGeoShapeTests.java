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
package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.test.geo.RandomShapeGenerator;

public class GeoShapeQueryBuilderGeoShapeTests extends GeoShapeQueryBuilderTests {

    protected String fieldName() {
        return GEO_SHAPE_FIELD_NAME;
    }

    protected GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        RandomShapeGenerator.ShapeType shapeType = randomFrom(
            RandomShapeGenerator.ShapeType.POINT,
            RandomShapeGenerator.ShapeType.MULTIPOINT,
            RandomShapeGenerator.ShapeType.LINESTRING,
            RandomShapeGenerator.ShapeType.MULTILINESTRING,
            RandomShapeGenerator.ShapeType.POLYGON);
        ShapeBuilder<?, ?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
        GeoShapeQueryBuilder builder;
        clearShapeFields();
        if (indexedShape == false) {
            builder = new GeoShapeQueryBuilder(fieldName(), shape);
        } else {
            indexedShapeToReturn = shape;
            indexedShapeId = randomAlphaOfLengthBetween(3, 20);
            builder = new GeoShapeQueryBuilder(fieldName(), indexedShapeId);
            if (randomBoolean()) {
                indexedShapeIndex = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeIndex(indexedShapeIndex);
            }
            if (randomBoolean()) {
                indexedShapePath = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapePath(indexedShapePath);
            }
            if (randomBoolean()) {
                indexedShapeRouting = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeRouting(indexedShapeRouting);
            }
        }
        if (randomBoolean()) {
            QueryShardContext context = createShardContext();
            if (context.indexVersionCreated().onOrAfter(Version.V_7_5_0)) { // CONTAINS is only supported from version 7.5
                if (shapeType == RandomShapeGenerator.ShapeType.LINESTRING || shapeType == RandomShapeGenerator.ShapeType.MULTILINESTRING) {
                    builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.CONTAINS));
                } else {
                    builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS,
                        ShapeRelation.WITHIN, ShapeRelation.CONTAINS));
                }
            } else {
                if (shapeType == RandomShapeGenerator.ShapeType.LINESTRING || shapeType == RandomShapeGenerator.ShapeType.MULTILINESTRING) {
                    builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS));
                } else {
                    builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN));
                }
            }
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
    }
}
