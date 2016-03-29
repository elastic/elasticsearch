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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Objects;

public class SamplerAggregatorBuilder extends AggregatorBuilder<SamplerAggregatorBuilder> {
    public static final int DEFAULT_SHARD_SAMPLE_SIZE = 100;

    private int shardSize = DEFAULT_SHARD_SAMPLE_SIZE;

    public SamplerAggregatorBuilder(String name) {
        super(name, InternalSampler.TYPE);
    }

    /**
     * Read from a stream.
     */
    SamplerAggregatorBuilder(StreamInput in) throws IOException {
        super(in, InternalSampler.TYPE);
        shardSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardSize);
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public SamplerAggregatorBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Get the max num docs to be returned from each shard.
     */
    public int shardSize() {
        return shardSize;
    }

    @Override
    protected SamplerAggregatorFactory doBuild(AggregationContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
            throws IOException {
        return new SamplerAggregatorFactory(name, type, shardSize, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SamplerAggregator.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        builder.endObject();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(shardSize);
    }

    @Override
    protected boolean doEquals(Object obj) {
        SamplerAggregatorBuilder other = (SamplerAggregatorBuilder) obj;
        return Objects.equals(shardSize, other.shardSize);
    }

}