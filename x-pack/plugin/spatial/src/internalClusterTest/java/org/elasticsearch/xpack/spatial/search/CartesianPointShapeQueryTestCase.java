/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.search.geo.BasePointShapeQueryTestCase;
import org.elasticsearch.search.geo.SpatialQueryBuilders;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;

public abstract class CartesianPointShapeQueryTestCase extends BasePointShapeQueryTestCase<ShapeQueryBuilder> {

    private final SpatialQueryBuilders<ShapeQueryBuilder> shapeQueryBuilder = TestSpatialQueryBuilders.CARTESIAN;

    @Override
    protected SpatialQueryBuilders<ShapeQueryBuilder> queryBuilder() {
        return shapeQueryBuilder;
    }

    @Override
    protected String fieldTypeName() {
        return "shape";
    }
}
