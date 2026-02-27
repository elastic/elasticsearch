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
import org.elasticsearch.geometry.Point;

public class SpatialPushDownGeoShapeIT extends SpatialPushDownShapeTestCase {

    @Override
    protected String fieldType() {
        return "geo_shape";
    }

    @Override
    protected Geometry getIndexGeometry() {
        return GeometryTestUtils.randomGeometryWithoutCircle(0, false);
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
    protected Point quantizePoint(Point point) {
        double lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getY()));
        double lon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getX()));
        return new Point(lon, lat);
    }

}
