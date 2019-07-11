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

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.ExecutionMode;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DiversifiedAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    private final int shardSize;
    private final int maxDocsPerValue;
    private final String executionHint;

    DiversifiedAggregatorFactory(String name, ValuesSourceConfig<ValuesSource> config, int shardSize, int maxDocsPerValue,
            String executionHint, SearchContext context, AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.shardSize = shardSize;
        this.maxDocsPerValue = maxDocsPerValue;
        this.executionHint = executionHint;
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        if (valuesSource instanceof ValuesSource.Numeric) {
            return new DiversifiedNumericSamplerAggregator(name, shardSize, factories, context, parent, pipelineAggregators, metaData,
                    (Numeric) valuesSource, maxDocsPerValue);
        }

        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint);
            }

            // In some cases using ordinals is just not supported: override
            // it
            if (execution == null) {
                execution = ExecutionMode.GLOBAL_ORDINALS;
            }
            if ((execution.needsGlobalOrdinals()) && (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals))) {
                execution = ExecutionMode.MAP;
            }
            return execution.create(name, factories, shardSize, maxDocsPerValue, valuesSource, context, parent, pipelineAggregators,
                    metaData);
        }

        throw new AggregationExecutionException("Sampler aggregation cannot be applied to field [" + config.fieldContext().field()
                + "]. It can only be applied to numeric or string fields.");
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        final UnmappedSampler aggregation = new UnmappedSampler(name, pipelineAggregators, metaData);

        return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

}
