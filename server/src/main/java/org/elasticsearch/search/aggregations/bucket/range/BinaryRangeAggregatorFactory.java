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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BinaryRangeAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(IpRangeAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.IP, BinaryRangeAggregator::new, true);
    }

    private final List<BinaryRangeAggregator.Range> ranges;
    private final boolean keyed;

    public BinaryRangeAggregatorFactory(String name,
            ValuesSourceConfig config,
            List<BinaryRangeAggregator.Range> ranges, boolean keyed,
            QueryShardContext queryShardContext,
            AggregatorFactory parent, Builder subFactoriesBuilder,
            Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.ranges = ranges;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        return new BinaryRangeAggregator(name, factories, null, config.format(),
                ranges, keyed, searchContext, parent, CardinalityUpperBound.NONE, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(IpRangeAggregationBuilder.REGISTRY_KEY, config)
            .build(name, factories, config.getValuesSource(), config.format(), ranges, keyed, searchContext, parent, cardinality, metadata);
    }

}
