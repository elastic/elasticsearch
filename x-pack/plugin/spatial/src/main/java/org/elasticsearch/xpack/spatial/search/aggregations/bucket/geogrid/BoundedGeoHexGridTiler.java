/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;

/**
 * Bounded geotile aggregation. It accepts hashes that intersects the provided bounds.
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
    protected boolean validAddress(String address) {
        return predicate.validAddress(address);
    }
}
