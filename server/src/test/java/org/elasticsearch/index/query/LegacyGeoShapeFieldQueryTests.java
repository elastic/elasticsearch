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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;

import java.io.IOException;

public class LegacyGeoShapeFieldQueryTests extends GeoShapeQueryBuilderTests {

    @Override
    protected String fieldName() {
        return GEO_SHAPE_FIELD_NAME;
    }

    @Override
    protected Settings createTestIndexSettings() {
        // force the legacy shape impl
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_5_0);
        return Settings.builder()
                .put(super.createTestIndexSettings())
                .put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .build();
    }

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder<?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
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
            SpatialStrategy strategy = randomFrom(SpatialStrategy.values());
            // ShapeType.MULTILINESTRING + SpatialStrategy.TERM can lead to large queries and will slow down tests, so
            // we try to avoid that combination
            while (shapeType == ShapeType.MULTILINESTRING && strategy == SpatialStrategy.TERM) {
                strategy = randomFrom(SpatialStrategy.values());
            }
            builder.strategy(strategy);
            if (strategy != SpatialStrategy.TERM) {
                builder.relation(randomFrom(ShapeRelation.values()));
            }
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
    }

    public void testInvalidRelation() throws IOException {
        ShapeBuilder<?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder.strategy(SpatialStrategy.TERM);
        expectThrows(IllegalArgumentException.class, () -> builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN)));
        GeoShapeQueryBuilder builder2 = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder2.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN));
        expectThrows(IllegalArgumentException.class, () -> builder2.strategy(SpatialStrategy.TERM));
        GeoShapeQueryBuilder builder3 = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder3.strategy(SpatialStrategy.TERM);
        expectThrows(IllegalArgumentException.class, () -> builder3.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN)));
    }
}
