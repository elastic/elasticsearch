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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, CardinalityAggregatorFactory> {

    private final Long precisionThreshold;

    public CardinalityAggregatorFactory(String name, Type type, ValuesSourceConfig<ValuesSource> config, Long precisionThreshold,
            AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder, metaData);
        this.precisionThreshold = precisionThreshold;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return new CardinalityAggregator(name, null, precision(parent), context, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new CardinalityAggregator(name, valuesSource, precision(parent), context, parent, pipelineAggregators,
                metaData);
    }

    private int precision(Aggregator parent) {
        return precisionThreshold == null ? defaultPrecision(parent) : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }

    /*
     * If one of the parent aggregators is a MULTI_BUCKET one, we might want to lower the precision
     * because otherwise it might be memory-intensive. On the other hand, for top-level aggregators
     * we try to focus on accuracy.
     */
    private static int defaultPrecision(Aggregator parent) {
        int precision = HyperLogLogPlusPlus.DEFAULT_PRECISION;
        while (parent != null) {
            if (parent instanceof SingleBucketAggregator == false) {
                // if the parent creates buckets, we subtract 5 to the precision,
                // which will effectively divide the memory usage of each counter by 32
                precision -= 5;
            }
            parent = parent.parent();
        }

        return Math.max(precision, HyperLogLogPlusPlus.MIN_PRECISION);
    }
}
