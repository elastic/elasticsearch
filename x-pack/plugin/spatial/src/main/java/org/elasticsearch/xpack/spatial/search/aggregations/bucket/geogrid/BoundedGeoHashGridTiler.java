/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;

/**
 * Bounded geotile aggregation. It accepts hashes that intersects the provided bounds.
 */
public class BoundedGeoHashGridTiler extends AbstractGeoHashGridTiler {
    private final GeoHashBoundedPredicate predicate;

    public BoundedGeoHashGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.predicate = new GeoHashBoundedPredicate(precision, bbox);
    }

    @Override
    protected long getMaxCells() {
        return predicate.getMaxHashes();
    }

    @Override
    protected boolean validHash(String hash) {
        return predicate.validHash(hash);
    }
}
