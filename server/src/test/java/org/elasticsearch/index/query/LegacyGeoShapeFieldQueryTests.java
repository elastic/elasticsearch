/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.TestLegacyGeoShapeFieldMapperPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class LegacyGeoShapeFieldQueryTests extends GeoShapeQueryBuilderTests {

    @SuppressWarnings("deprecation")
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(TestLegacyGeoShapeFieldMapperPlugin.class);
    }

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
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build();
    }

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        GeoShapeQueryBuilder builder;
        clearShapeFields();
        if (indexedShape == false) {
            builder = new GeoShapeQueryBuilder(fieldName(), geometry);
        } else {
            indexedShapeToReturn = geometry;
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
            while (geometry.type() == ShapeType.MULTILINESTRING && strategy == SpatialStrategy.TERM) {
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
        Geometry shape = GeometryTestUtils.randomGeometry(false);
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
