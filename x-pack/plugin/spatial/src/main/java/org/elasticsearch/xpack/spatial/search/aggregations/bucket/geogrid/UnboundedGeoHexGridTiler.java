/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

/**
 * Unbounded geohash aggregation. It accepts any hash.
 */
public class UnboundedGeoHexGridTiler extends AbstractGeoHexGridTiler {

    private final long maxAddresses;

    public UnboundedGeoHexGridTiler(int precision) {
        super(precision);
        // TODO: Verify this (and perhaps move the calculation into H3 and based on NUM_BASE_CELLS and others)
        maxAddresses = 122 + (long) Math.pow(7, precision);
    }

    @Override
    protected boolean validAddress(String address) {
        return true;
    }

    @Override
    protected long getMaxCells() {
        return maxAddresses;
    }
}
