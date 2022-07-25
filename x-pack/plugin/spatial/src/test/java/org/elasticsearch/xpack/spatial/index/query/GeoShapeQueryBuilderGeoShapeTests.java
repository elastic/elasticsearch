/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoShapeQueryBuilderTestCase;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class GeoShapeQueryBuilderGeoShapeTests extends GeoShapeQueryBuilderTestCase {

    private static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";

    protected static final String GEO_SHAPE_ALIAS_FIELD_NAME = "mapped_geo_shape_alias";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        final XContentBuilder builder = PutMappingRequest.simpleMapping(
            GEO_SHAPE_FIELD_NAME,
            "type=geo_shape",
            GEO_SHAPE_ALIAS_FIELD_NAME,
            "type=alias,path=" + GEO_SHAPE_FIELD_NAME
        );
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(builder)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(LocalStateSpatialPlugin.class);
    }

    @Override
    protected String getFieldName() {
        return randomFrom(GEO_SHAPE_FIELD_NAME, GEO_SHAPE_ALIAS_FIELD_NAME);
    }

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        ShapeType shapeType = ESTestCase.randomFrom(
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
            builder = new GeoShapeQueryBuilder(getFieldName(), geometry);
        } else {
            indexedShapeToReturn = geometry;
            indexedShapeId = ESTestCase.randomAlphaOfLengthBetween(3, 20);
            builder = new GeoShapeQueryBuilder(getFieldName(), indexedShapeId);
            if (ESTestCase.randomBoolean()) {
                indexedShapeIndex = ESTestCase.randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeIndex(indexedShapeIndex);
            }
            if (ESTestCase.randomBoolean()) {
                indexedShapePath = ESTestCase.randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapePath(indexedShapePath);
            }
            if (ESTestCase.randomBoolean()) {
                indexedShapeRouting = ESTestCase.randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeRouting(indexedShapeRouting);
            }
        }
        if (ESTestCase.randomBoolean()) {
            SearchExecutionContext context = AbstractBuilderTestCase.createSearchExecutionContext();
            if (context.indexVersionCreated().onOrAfter(Version.V_7_5_0)) { // CONTAINS is only supported from version 7.5
                if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
                    builder.relation(ESTestCase.randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.CONTAINS));
                } else {
                    builder.relation(
                        ESTestCase.randomFrom(
                            ShapeRelation.DISJOINT,
                            ShapeRelation.INTERSECTS,
                            ShapeRelation.WITHIN,
                            ShapeRelation.CONTAINS
                        )
                    );
                }
            } else {
                if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
                    builder.relation(ESTestCase.randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS));
                } else {
                    builder.relation(ESTestCase.randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN));
                }
            }
        }

        if (ESTestCase.randomBoolean()) {
            builder.ignoreUnmapped(ESTestCase.randomBoolean());
        }
        return builder;
    }
}
