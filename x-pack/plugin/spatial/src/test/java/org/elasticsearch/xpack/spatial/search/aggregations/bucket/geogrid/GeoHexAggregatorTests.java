/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregatorTestCase;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.List;

public class GeoHexAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoHexGridBucket> {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(GeoShapeValuesSourceType.instance(), CoreValuesSourceType.GEOPOINT);
    }

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, H3.MAX_H3_RES);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return H3.geoToH3Address(lat, lng, precision);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHexGridAggregationBuilder(name);
    }

    @Override
    protected Point randomPoint() {
        return GeometryTestUtils.randomPoint();
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        return GeoTestUtils.randomBBox();
    }

    @Override
    protected Rectangle getTile(double lng, double lat, int precision) {
        CellBoundary boundary = H3.h3ToGeoBoundary(hashAsString(lng, lat, precision));
        double minLat = Double.POSITIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < boundary.numPoints(); i++) {
            double boundaryLat = boundary.getLatLon(i).getLatDeg();
            double boundaryLon = boundary.getLatLon(i).getLonDeg();
            minLon = Math.min(minLon, boundaryLon);
            maxLon = Math.max(maxLon, boundaryLon);
            minLat = Math.min(minLat, boundaryLat);
            maxLat = Math.max(maxLat, boundaryLat);
        }
        return new Rectangle(minLon, maxLon, maxLat, minLat);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createBuilder("foo").field(fieldName);
    }
}
