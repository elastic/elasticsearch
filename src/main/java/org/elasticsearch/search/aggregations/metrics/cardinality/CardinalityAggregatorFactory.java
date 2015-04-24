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

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

final class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    private final long precisionThreshold;
    private final boolean rehash;

    CardinalityAggregatorFactory(String name, ValuesSourceConfig config, long precisionThreshold, boolean rehash) {
        super(name, InternalCardinality.TYPE.name(), config);
        this.precisionThreshold = precisionThreshold;
        this.rehash = rehash;
    }

    private int precision(Aggregator parent) {
        return precisionThreshold < 0 ? defaultPrecision(parent) : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        return new CardinalityAggregator(name, null, true, precision(parent), config.formatter(), context, parent, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
        if (!(valuesSource instanceof ValuesSource.Numeric) && !rehash) {
            throw new AggregationExecutionException("Turning off rehashing for cardinality aggregation [" + name + "] on non-numeric values in not allowed");
        }
        return new CardinalityAggregator(name, valuesSource, rehash, precision(parent), config.formatter(), context, parent, metaData);
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
                // if the parent creates buckets, we substract 5 to the precision,
                // which will effectively divide the memory usage of each counter by 32
                precision -= 5;
            }
            parent = parent.parent();
        }

        return Math.max(precision, HyperLogLogPlusPlus.MIN_PRECISION);
    }

}
