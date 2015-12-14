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
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link Sampler} aggregation.
 */
public class DiversifiedSamplerAggregationBuilder extends ValuesSourceAggregationBuilder<DiversifiedSamplerAggregationBuilder> {

    private int shardSize = SamplerAggregator.Factory.DEFAULT_SHARD_SAMPLE_SIZE;

    int maxDocsPerValue = SamplerAggregator.DiversifiedFactory.MAX_DOCS_PER_VALUE_DEFAULT;
    String executionHint = null;

    /**
     * Sole constructor.
     */
    public DiversifiedSamplerAggregationBuilder(String name) {
        super(name, SamplerAggregator.DiversifiedFactory.TYPE.name());
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public DiversifiedSamplerAggregationBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    public DiversifiedSamplerAggregationBuilder maxDocsPerValue(int maxDocsPerValue) {
        this.maxDocsPerValue = maxDocsPerValue;
        return this;
    }

    public DiversifiedSamplerAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (shardSize != SamplerAggregator.Factory.DEFAULT_SHARD_SAMPLE_SIZE) {
            builder.field(SamplerAggregator.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        }

        if (maxDocsPerValue != SamplerAggregator.DiversifiedFactory.MAX_DOCS_PER_VALUE_DEFAULT) {
            builder.field(SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD.getPreferredName(), maxDocsPerValue);
        }
        if (executionHint != null) {
            builder.field(SamplerAggregator.EXECUTION_HINT_FIELD.getPreferredName(), executionHint);
        }

        return builder;
    }


}
