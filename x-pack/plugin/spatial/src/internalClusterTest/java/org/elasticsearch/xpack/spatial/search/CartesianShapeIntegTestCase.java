/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.search.geo.BaseShapeIntegTestCase;
import org.elasticsearch.search.geo.SpatialQueryBuilders;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public abstract class CartesianShapeIntegTestCase extends BaseShapeIntegTestCase<ShapeQueryBuilder> {

    protected SpatialQueryBuilders<ShapeQueryBuilder> shapeQueryBuilder = TestSpatialQueryBuilders.CARTESIAN;

    @Override
    protected SpatialQueryBuilders<ShapeQueryBuilder> queryBuilder() {
        return shapeQueryBuilder;
    }

    @Override
    protected String getFieldTypeName() {
        return "point";
    }

    protected void doDistanceAndBoundingBoxTest(String key) {
        // TODO: Support distance and bounding box queries on Cartesian data
    }

    /** We override this method to convert test that that is geo-specific to work with cartesian tests */
    protected byte[] convertTestData(ByteArrayOutputStream out) {
        String data = out.toString(StandardCharsets.UTF_8).replaceAll("\"lat\"", "\"y\"").replaceAll("\"lon\"", "\"x\"");
        return data.getBytes(StandardCharsets.UTF_8);
    }
}
