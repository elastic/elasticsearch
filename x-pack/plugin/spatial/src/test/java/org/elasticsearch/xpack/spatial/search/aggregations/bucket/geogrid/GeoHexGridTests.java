/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.h3.H3;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.List;
import java.util.Map;

public class GeoHexGridTests extends GeoGridTestCase<InternalGeoHexGridBucket, InternalGeoHexGrid> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new LocalStateSpatialPlugin();
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(GeoHexGridAggregationBuilder.NAME),
                (p, c) -> ParsedGeoHexGrid.fromXContent(p, (String) c)
            )
        );
    }

    @Override
    protected InternalGeoHexGrid createInternalGeoGrid(
        String name,
        int size,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new InternalGeoHexGrid(name, size, buckets, metadata);
    }

    @Override
    protected InternalGeoHexGridBucket createInternalGeoGridBucket(Long key, long docCount, InternalAggregations aggregations) {
        return new InternalGeoHexGridBucket(key, docCount, aggregations);
    }

    @Override
    protected long longEncode(double lng, double lat, int precision) {
        return H3.geoToH3(lat, lng, precision);
    }

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, H3.MAX_H3_RES);
    }
}
