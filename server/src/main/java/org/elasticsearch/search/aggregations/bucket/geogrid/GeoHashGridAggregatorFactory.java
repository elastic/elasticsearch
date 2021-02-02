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

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.metrics.GeoGridAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyList;

public class GeoHashGridAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final GeoGridAggregatorSupplier aggregatorSupplier;
    private final int precision;
    private final int requiredSize;
    private final int shardSize;
    private final GeoBoundingBox geoBoundingBox;

    GeoHashGridAggregatorFactory(String name, ValuesSourceConfig config, int precision, int requiredSize,
                                 int shardSize, GeoBoundingBox geoBoundingBox, AggregationContext context,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                 Map<String, Object> metadata,
                                 GeoGridAggregatorSupplier aggregatorSupplier) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.aggregatorSupplier = aggregatorSupplier;
        this.precision = precision;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.geoBoundingBox = geoBoundingBox;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new InternalGeoHashGrid(name, requiredSize, emptyList(), metadata);
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        return aggregatorSupplier
            .build(
                name,
                factories,
                config.getValuesSource(),
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                context,
                parent,
                cardinality,
                metadata
            );
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            GeoHashGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                context,
                parent,
                cardinality,
                metadata) -> {
                CellIdSource cellIdSource = new CellIdSource(
                    (ValuesSource.GeoPoint) valuesSource,
                    precision,
                    geoBoundingBox,
                    Geohash::longEncode
                );
                return new GeoHashGridAggregator(
                    name,
                    factories,
                    cellIdSource,
                    requiredSize,
                    shardSize,
                    context,
                    parent,
                    cardinality,
                    metadata
                );
            },
                true);
    }
}
