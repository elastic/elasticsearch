/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class GeoShapeQueryBuilderGeoShapeTests extends GeoShapeQueryBuilderTests {

    private static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder builder = PutMappingRequest.simpleMapping(GEO_SHAPE_FIELD_NAME, "type=geo_shape");
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(builder)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @SuppressWarnings("deprecation") // dependencies in server for geo_shape field should be decoupled
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(TestGeoShapeFieldMapperPlugin.class);
    }

    protected String fieldName() {
        return GEO_SHAPE_FIELD_NAME;
    }

    protected GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        ShapeType shapeType = randomFrom(
            ShapeType.POINT,
            ShapeType.MULTIPOINT,
            ShapeType.LINESTRING,
            ShapeType.MULTILINESTRING,
            ShapeType.POLYGON
        );
        Geometry geometry = GeometryTestUtils.randomGeometry(shapeType, false);
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
            SearchExecutionContext context = createSearchExecutionContext();
            if (context.indexVersionCreated().onOrAfter(Version.V_7_5_0)) { // CONTAINS is only supported from version 7.5
                if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
                    builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.CONTAINS));
                } else {
                    builder.relation(
                        randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN, ShapeRelation.CONTAINS)
                    );
                }
            } else {
                if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
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
