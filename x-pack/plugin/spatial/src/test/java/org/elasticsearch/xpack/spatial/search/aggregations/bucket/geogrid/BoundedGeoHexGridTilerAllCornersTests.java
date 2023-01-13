/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;

import static org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler.BoundedGeoHexGridTiler.wAvg;
import static org.hamcrest.Matchers.equalTo;

/**
 * The new algorithm calculates the inflation as a weighted average of the original bounds
 * and the bounds of the cells at all four corners of the bbox.
 */
public class BoundedGeoHexGridTilerAllCornersTests extends BoundedGeoHexGridTilerTests {

    @Override
    protected GeoBoundingBox inflateBbox(int precision, GeoBoundingBox bbox, double factor) {
        return GeoHexGridTiler.BoundedGeoHexGridTiler.inflateBbox2(precision, bbox, factor);
    }

    @Override
    /* Calculate the bounds of the h3 cell assuming the test bbox is entirely within the cell */
    protected Rectangle getFullBounds(Rectangle bounds, GeoBoundingBox bbox) {
        return bounds;
    }

    public void testBoundedTilerInflation_WeightedAverages() {
        assertThat("Weighted average 0.5 (pos)", wAvg(10, 20, 0.5), equalTo(15.0));
        assertThat("Weighted average 0.5 (neg)", wAvg(-10, -20, 0.5), equalTo(-15.0));
        assertThat("Weighted average 0.5", wAvg(-10, 10, 0.5), equalTo(0.0));
        assertThat("Weighted average 0.0 (pos)", wAvg(10, 20, 0.0), equalTo(10.0));
        assertThat("Weighted average 0.0 (neg)", wAvg(-10, -20, 0.0), equalTo(-10.0));
        assertThat("Weighted average 0.0", wAvg(-10, 10, 0.0), equalTo(-10.0));
        assertThat("Weighted average 1.0 (pos)", wAvg(10, 20, 1.0), equalTo(20.0));
        assertThat("Weighted average 1.0 (neg)", wAvg(-10, -20, 1.0), equalTo(-20.0));
        assertThat("Weighted average 1.0", wAvg(-10, 10, 1.0), equalTo(10.0));
        assertThat("Weighted average 2.0 (pos)", wAvg(10, 20, 2.0), equalTo(30.0));
        assertThat("Weighted average 2.0 (neg)", wAvg(-10, -20, 2.0), equalTo(-30.0));
        assertThat("Weighted average 2.0", wAvg(-10, 10, 2.0), equalTo(30.0));
    }
}
