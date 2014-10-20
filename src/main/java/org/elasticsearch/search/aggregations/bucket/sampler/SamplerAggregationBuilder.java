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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link Sampler} aggregation.
 */
public class SamplerAggregationBuilder extends AggregationBuilder<SamplerAggregationBuilder> {


    private int shardSize = SamplerParser.DEFAULT_SHARD_SAMPLE_SIZE;
    private boolean useRandomSample = SamplerParser.DEFAULT_USE_RANDOM_SAMPLE;

    /**
     * Sole constructor.
     */
    public SamplerAggregationBuilder(String name) {
        super(name, InternalSampler.TYPE.name());
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public SamplerAggregationBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Set to true if the sample should be based on a random sample, false to
     * sample based on the top-scoring documents. Default is false.
     */
    public SamplerAggregationBuilder useRandomSample(boolean useRandomSample) {
        this.useRandomSample = useRandomSample;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (shardSize != SamplerParser.DEFAULT_SHARD_SAMPLE_SIZE) {
            builder.field(SamplerParser.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        }

        if (useRandomSample != SamplerParser.DEFAULT_USE_RANDOM_SAMPLE) {
            builder.field(SamplerParser.RANDOM_SAMPLE_FIELD.getPreferredName(), useRandomSample);
        }

        return builder.endObject();
    }
}
