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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FiltersAggregatorFactory extends AggregatorFactory<FiltersAggregatorFactory> {

    private final String[] keys;
    private final Weight[] weights;
    private final boolean keyed;
    private final boolean otherBucket;
    private final String otherBucketKey;

    public FiltersAggregatorFactory(String name, Type type, List<KeyedFilter> filters, boolean keyed, boolean otherBucket,
            String otherBucketKey, AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactories,
            Map<String, Object> metaData) throws IOException {
        super(name, type, context, parent, subFactories, metaData);
        this.keyed = keyed;
        this.otherBucket = otherBucket;
        this.otherBucketKey = otherBucketKey;
        IndexSearcher contextSearcher = context.searchContext().searcher();
        weights = new Weight[filters.size()];
        keys = new String[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            this.keys[i] = keyedFilter.key();
            Query filter = keyedFilter.filter().toFilter(context.searchContext().getQueryShardContext());
            this.weights[i] = contextSearcher.createNormalizedWeight(filter, false);
        }
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new FiltersAggregator(name, factories, keys, weights, keyed, otherBucket ? otherBucketKey : null, context, parent,
                pipelineAggregators, metaData);
    }


}
