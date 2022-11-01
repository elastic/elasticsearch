/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;

/**
 * Bounded geohex aggregation. It accepts H3 addresses that intersect the provided bounds.
 * The additional support for testing intersection with inflated H3 cells is used when testing
 * parent cells, since child cells can exceed the bounds of their parent. We inflate the parent
 * cells by 1.17 to encompass all children. This is particularly important when using this for
 * MVT visualization, as otherwise it can knock out large chunks of polygons when zoomed in.
 */
public class BoundedGeoHexGridTiler extends AbstractGeoHexGridTiler {
    private final GeoHexBoundedPredicate predicate;

    public BoundedGeoHexGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.predicate = new GeoHexBoundedPredicate(precision, bbox);
    }

    @Override
    protected long getMaxCells() {
        return predicate.getMaxCells();
    }

    @Override
    protected boolean cellIntersectsBounds(String address) {
        return predicate.validAddress(address);
    }

    @Override
    protected boolean cellIntersectsBounds(String address, double scaleFactor) {
        return predicate.validAddress(address, scaleFactor);
    }
}
