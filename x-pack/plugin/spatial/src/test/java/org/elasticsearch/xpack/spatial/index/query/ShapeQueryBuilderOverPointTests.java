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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.io.IOException;


public class ShapeQueryBuilderOverPointTests extends ShapeQueryBuilderTests {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(docType, new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
            fieldName(), "type=point"))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected ShapeRelation getShapeRelation(ShapeType type) {
        return ShapeRelation.INTERSECTS;
    }

    @Override
    protected Geometry getGeometry() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return ShapeTestUtils.randomMultiPolygon(false);
            } else {
                return ShapeTestUtils.randomPolygon(false);
            }
        } else if (randomBoolean()) {
            // it should be a circle
            return ShapeTestUtils.randomPolygon(false);
        } else {
            return ShapeTestUtils.randomRectangle();
        }
    }
}
