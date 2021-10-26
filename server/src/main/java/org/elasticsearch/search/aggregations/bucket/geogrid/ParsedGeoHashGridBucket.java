/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoHashGridBucket extends ParsedGeoGridBucket {

    @Override
    public GeoPoint getKey() {
        return GeoPoint.fromGeohash(hashAsString);
    }

    @Override
    public String getKeyAsString() {
        return hashAsString;
    }

    static ParsedGeoHashGridBucket fromXContent(XContentParser parser) throws IOException {
        return parseXContent(parser, false, ParsedGeoHashGridBucket::new, (p, bucket) -> bucket.hashAsString = p.textOrNull());
    }
}
