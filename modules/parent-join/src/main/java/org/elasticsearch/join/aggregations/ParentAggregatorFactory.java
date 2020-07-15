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

package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.AggregationUsageService.OTHER_SUBTYPE;

public class ParentAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Query parentFilter;
    private final Query childFilter;

    public ParentAggregatorFactory(String name,
                                   ValuesSourceConfig config,
                                   Query childFilter,
                                   Query parentFilter,
                                   QueryShardContext queryShardContext,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);

        this.childFilter = childFilter;
        this.parentFilter = parentFilter;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new NonCollectingAggregator(name, searchContext, parent, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalParent(name, 0, buildEmptySubAggregations(), metadata());
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext, Aggregator children,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {

        ValuesSource rawValuesSource = config.getValuesSource();
        if (rawValuesSource instanceof WithOrdinals == false) {
            throw new AggregationExecutionException("ValuesSource type " + rawValuesSource.toString() +
                "is not supported for aggregation " + this.name());
        }
        WithOrdinals valuesSource = (WithOrdinals) rawValuesSource;
        long maxOrd = valuesSource.globalMaxOrd(searchContext.searcher());
        return new ChildrenToParentAggregator(name, factories, searchContext, children, childFilter,
            parentFilter, valuesSource, maxOrd, cardinality, metadata);
    }

    @Override
    public String getStatsSubtype() {
        // Parent Aggregation is registered in non-standard way
        return OTHER_SUBTYPE;
    }
}
