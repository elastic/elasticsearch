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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class HistogramAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, HistogramAggregatorFactory> {

    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;

    public HistogramAggregatorFactory(String name, ValuesSourceConfig<ValuesSource> config, double interval, double offset,
            BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
            SearchContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        if (valuesSource instanceof ValuesSource.Numeric) {
            return createAggregator((ValuesSource.Numeric) valuesSource, parent, pipelineAggregators, metaData);
        }
        else if (valuesSource instanceof ValuesSource.Bytes) {
            return createAggregator((ValuesSource.Bytes) valuesSource, parent, pipelineAggregators, metaData);
        }
        else {
            throw new IllegalArgumentException("Expected one of [Numeric, Bytes] values source, found [" + valuesSource.toString() + "]");
        }
    }

    private Aggregator createAggregator(ValuesSource.Numeric valuesSource, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {

        return new NumericHistogramAggregator(name, factories, interval, offset, order, keyed, minDocCount, minBound, maxBound, valuesSource,
                config.format(), context, parent, pipelineAggregators, metaData);
    }

    private Aggregator createAggregator(ValuesSource.Bytes valuesSource, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {

        return new RangeHistogramAggregator(name, factories, interval, offset, order, keyed, minDocCount, minBound, maxBound, valuesSource,
            config.format(), context, parent, pipelineAggregators, metaData);
    }


    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return createAggregator((ValuesSource.Numeric) null, parent, pipelineAggregators, metaData);
    }
}
