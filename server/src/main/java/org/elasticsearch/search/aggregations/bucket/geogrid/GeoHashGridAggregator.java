/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as GeoHash longs (for efficiency's sake) but formats results as Geohash strings.
 */
public class GeoHashGridAggregator extends GeoGridAggregator<InternalGeoHashGrid> {

    public GeoHashGridAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
                                 int requiredSize, int shardSize, SearchContext aggregationContext,
                                 Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, factories, valuesSource, requiredSize, shardSize, aggregationContext, parent, metadata);
    }

    @Override
    InternalGeoHashGrid buildAggregation(String name, int requiredSize, List<InternalGeoGridBucket> buckets,
                                         Map<String, Object> metadata) {
        return new InternalGeoHashGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public InternalGeoHashGrid buildEmptyAggregation() {
        return new InternalGeoHashGrid(name, requiredSize, Collections.emptyList(), metadata());
    }

    InternalGeoGridBucket newEmptyBucket() {
        return new InternalGeoHashGridBucket(0, 0, null);
    }
}
