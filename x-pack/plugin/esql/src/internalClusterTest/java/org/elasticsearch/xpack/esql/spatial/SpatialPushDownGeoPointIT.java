/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;

public class SpatialPushDownGeoPointIT extends SpatialPushDownTestCase {

    @Override
    protected String fieldType() {
        return "geo_point";
    }

    @Override
    protected Geometry getIndexGeometry() {
        return GeometryTestUtils.randomPoint();
    }

    @Override
    protected Geometry getQueryGeometry() {
        return GeometryTestUtils.randomGeometry(false);
    }

    @Override
    protected String castingFunction() {
        return "TO_GEOSHAPE";
    }
}
