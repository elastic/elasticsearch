/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;

public class SpatialPushDownGeoPointIT extends SpatialPushDownPointsTestCase {

    private static final double LAT_MAX_VALUE = GeoEncodingUtils.decodeLatitude(Integer.MAX_VALUE - 3);

    @Override
    protected String fieldType() {
        return "geo_point";
    }

    @Override
    protected Geometry getIndexGeometry() {
        // This is to overcome lucene bug https://github.com/apache/lucene/issues/13703.
        // Once it is fixed we can remove this workaround.
        return randomValueOtherThanMany(p -> p.getLat() >= LAT_MAX_VALUE, GeometryTestUtils::randomPoint);
    }

    @Override
    protected Geometry getQueryGeometry() {
        return GeometryTestUtils.randomGeometry(false);
    }

    @Override
    protected String castingFunction() {
        return "TO_GEOSHAPE";
    }

    @Override
    protected double searchDistance() {
        return 10000000;
    }
}
