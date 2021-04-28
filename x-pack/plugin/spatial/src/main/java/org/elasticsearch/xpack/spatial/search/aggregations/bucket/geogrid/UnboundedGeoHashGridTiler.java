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
public class UnboundedGeoHashGridTiler extends AbstractGeoHashGridTiler {

    public UnboundedGeoHashGridTiler(int precision) {
        super(precision);
    }

    @Override
    protected boolean validHash(String hash) {
       return true;
    }
}
