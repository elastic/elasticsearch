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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public abstract class ValuesSourceAggregatorFactory extends AggregatorFactory {

    protected ValuesSourceConfig config;

    public ValuesSourceAggregatorFactory(String name, ValuesSourceConfig config, QueryShardContext queryShardContext,
                                         AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                         Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.config = config;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality,
                                     Map<String, Object> metadata) throws IOException {
        if (config.hasValues() == false) {
            return createUnmapped(searchContext, parent, metadata);
        }
        return doCreateInternal(searchContext, parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that
     * doesn't have values.
     */
    protected abstract Aggregator createUnmapped(SearchContext searchContext,
                                                 Aggregator parent,
                                                 Map<String, Object> metadata) throws IOException;

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that has
     * values.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(SearchContext searchContext,
                                                   Aggregator parent,
                                                   CardinalityUpperBound cardinality,
                                                   Map<String, Object> metadata) throws IOException;

    @Override
    public String getStatsSubtype() {
        return config.valueSourceType().typeName();
    }
}
