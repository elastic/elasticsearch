/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoGridBucket;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoHexGridBucket extends ParsedGeoGridBucket {

    @Override
    public GeoPoint getKey() {
        LatLng latLng = H3.h3ToLatLng(hashAsString);
        return new GeoPoint(latLng.getLatDeg(), latLng.getLonDeg());
    }

    @Override
    public String getKeyAsString() {
        return hashAsString;
    }

    static ParsedGeoHexGridBucket fromXContent(XContentParser parser) throws IOException {
        return parseXContent(parser, false, ParsedGeoHexGridBucket::new, (p, bucket) -> bucket.hashAsString = p.text());
    }
}
