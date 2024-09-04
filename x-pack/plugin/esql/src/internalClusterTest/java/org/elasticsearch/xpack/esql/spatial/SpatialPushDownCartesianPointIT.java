/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;

public class SpatialPushDownCartesianPointIT extends SpatialPushDownTestCase {

    @Override
    protected String fieldType() {
        return "point";
    }

    @Override
    protected Geometry getIndexGeometry() {
        return ShapeTestUtils.randomPoint();
    }

    @Override
    protected Geometry getQueryGeometry() {
        return ShapeTestUtils.randomGeometry(false);
    }

    @Override
    protected String castingFunction() {
        return "TO_CARTESIANSHAPE";
    }
}
