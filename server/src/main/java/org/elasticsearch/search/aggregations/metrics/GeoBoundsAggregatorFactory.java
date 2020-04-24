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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class GeoBoundsAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final boolean wrapLongitude;

    GeoBoundsAggregatorFactory(String name,
                                ValuesSourceConfig config,
                                boolean wrapLongitude,
                                QueryShardContext queryShardContext,
                                AggregatorFactory parent,
                                AggregatorFactories.Builder subFactoriesBuilder,
                                Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.wrapLongitude = wrapLongitude;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new GeoBoundsAggregator(name, searchContext, parent, null, wrapLongitude, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(config.valueSourceType(), GeoBoundsAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof GeoBoundsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected "
                + GeoBoundsAggregatorSupplier.class.getName() + ", found [" + aggregatorSupplier.getClass().toString() + "]");
        }

        return ((GeoBoundsAggregatorSupplier) aggregatorSupplier).build(name, searchContext, parent, valuesSource, wrapLongitude,
            metadata);
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(GeoBoundsAggregationBuilder.NAME, CoreValuesSourceType.GEOPOINT,
            (GeoBoundsAggregatorSupplier) (name, aggregationContext, parent, valuesSource, wrapLongitude, metadata)
                -> new GeoBoundsAggregator(name, aggregationContext, parent, (ValuesSource.GeoPoint) valuesSource,
                    wrapLongitude, metadata));
    }
}
