/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

public class ShapeQueryBuilderOverPointTests extends ShapeQueryBuilderTests {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            docType,
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(fieldName(), "type=point"))),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected ShapeRelation getShapeRelation(ShapeType type) {
        return randomFrom(ShapeRelation.INTERSECTS, ShapeRelation.CONTAINS, ShapeRelation.DISJOINT, ShapeRelation.WITHIN);
    }

    @Override
    protected Geometry getGeometry() {
        return ShapeTestUtils.randomGeometry(false);
    }
}
