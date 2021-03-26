/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;

public class GeoHashGridAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoHashGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(1, 12);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return stringEncode(lng, lat, precision);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }
}
