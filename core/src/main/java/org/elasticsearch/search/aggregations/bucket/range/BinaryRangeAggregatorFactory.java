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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public class BinaryRangeAggregatorFactory
        extends ValuesSourceAggregatorFactory<ValuesSource.Bytes, BinaryRangeAggregatorFactory> {

    private final List<BinaryRangeAggregator.Range> ranges;
    private final boolean keyed;

    public BinaryRangeAggregatorFactory(String name, Type type,
            ValuesSourceConfig<ValuesSource.Bytes> config,
            List<BinaryRangeAggregator.Range> ranges, boolean keyed,
            AggregationContext context,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder, metaData);
        this.ranges = ranges;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new BinaryRangeAggregator(name, factories, null, config.format(),
                ranges, keyed, context, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Bytes valuesSource,
            Aggregator parent,
            boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new BinaryRangeAggregator(name, factories, valuesSource, config.format(),
                ranges, keyed, context, parent, pipelineAggregators, metaData);
    }

}
