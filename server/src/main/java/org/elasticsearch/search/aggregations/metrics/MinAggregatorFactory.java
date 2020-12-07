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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class MinAggregatorFactory extends ValuesSourceAggregatorFactory {

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MinAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            MinAggregator::new,
            true);
    }

    MinAggregatorFactory(String name, ValuesSourceConfig config, AggregationContext context,
                         AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                         Map<String, Object> metadata) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalMin(name, Double.POSITIVE_INFINITY, config.format(), metadata());
            }

            @Override
            public final LeafBucketCollector getLeafCollector(LeafReaderContext reader, LeafBucketCollector sub) {
                if (parent == null) {
                    return LeafBucketCollector.NO_OP_COLLECTOR;
                } else {
                    // we have no parent and the values source is empty so we can skip collecting hits.
                    throw new CollectionTerminatedException();
                }
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return context.getValuesSourceRegistry()
            .getAggregator(MinAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, searchContext, parent, metadata);
    }
}
