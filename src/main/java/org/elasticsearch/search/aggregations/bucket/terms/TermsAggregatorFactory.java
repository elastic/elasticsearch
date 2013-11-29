/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

/**
 *
 */
public class TermsAggregatorFactory extends ValueSourceAggregatorFactory {

    private final InternalOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final IncludeExclude includeExclude;

    public TermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, InternalOrder order, int requiredSize, int shardSize, IncludeExclude includeExclude) {
        super(name, StringTerms.TYPE.name(), valueSourceConfig);
        this.order = order;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.includeExclude = includeExclude;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        return new UnmappedTermsAggregator(name, order, requiredSize, aggregationContext, parent);
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
        if (valuesSource instanceof BytesValuesSource) {
            return new StringTermsAggregator(name, factories, valuesSource, order, requiredSize, shardSize, includeExclude, aggregationContext, parent);
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof NumericValuesSource) {
            if (((NumericValuesSource) valuesSource).isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, shardSize, aggregationContext, parent);
            }
            return new LongTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, shardSize, aggregationContext, parent);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + valuesSourceConfig.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

}
