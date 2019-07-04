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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class DiversifiedAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, DiversifiedAggregationBuilder> {
    public static final String NAME = "diversified_sampler";

    public static final int MAX_DOCS_PER_VALUE_DEFAULT = 1;

    private static final ObjectParser<DiversifiedAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(DiversifiedAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, false);
        PARSER.declareInt(DiversifiedAggregationBuilder::shardSize, SamplerAggregator.SHARD_SIZE_FIELD);
        PARSER.declareInt(DiversifiedAggregationBuilder::maxDocsPerValue, SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD);
        PARSER.declareString(DiversifiedAggregationBuilder::executionHint, SamplerAggregator.EXECUTION_HINT_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new DiversifiedAggregationBuilder(aggregationName), null);
    }

    private int shardSize = SamplerAggregationBuilder.DEFAULT_SHARD_SAMPLE_SIZE;
    private int maxDocsPerValue = MAX_DOCS_PER_VALUE_DEFAULT;
    private String executionHint = null;

    public DiversifiedAggregationBuilder(String name) {
        super(name, ValuesSourceType.ANY, null);
    }

    protected DiversifiedAggregationBuilder(DiversifiedAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.shardSize = clone.shardSize;
        this.maxDocsPerValue = clone.maxDocsPerValue;
        this.executionHint = clone.executionHint;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new DiversifiedAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public DiversifiedAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.ANY, null);
        shardSize = in.readVInt();
        maxDocsPerValue = in.readVInt();
        executionHint = in.readOptionalString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(shardSize);
        out.writeVInt(maxDocsPerValue);
        out.writeOptionalString(executionHint);
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public DiversifiedAggregationBuilder shardSize(int shardSize) {
        if (shardSize < 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than or equal to 0. Found [" + shardSize + "] in [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Get the max num docs to be returned from each shard.
     */
    public int shardSize() {
        return shardSize;
    }

    /**
     * Set the max num docs to be returned per value.
     */
    public DiversifiedAggregationBuilder maxDocsPerValue(int maxDocsPerValue) {
        if (maxDocsPerValue < 0) {
            throw new IllegalArgumentException(
                    "[maxDocsPerValue] must be greater than or equal to 0. Found [" + maxDocsPerValue + "] in [" + name + "]");
        }
        this.maxDocsPerValue = maxDocsPerValue;
        return this;
    }

    /**
     * Get the max num docs to be returned per value.
     */
    public int maxDocsPerValue() {
        return maxDocsPerValue;
    }

    /**
     * Set the execution hint.
     */
    public DiversifiedAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Get the execution hint.
     */
    public String executionHint() {
        return executionHint;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(SearchContext context,
            ValuesSourceConfig<ValuesSource> config, AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new DiversifiedAggregatorFactory(name, config, shardSize, maxDocsPerValue, executionHint, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(SamplerAggregator.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        builder.field(SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD.getPreferredName(), maxDocsPerValue);
        if (executionHint != null) {
            builder.field(SamplerAggregator.EXECUTION_HINT_FIELD.getPreferredName(), executionHint);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shardSize, maxDocsPerValue, executionHint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        DiversifiedAggregationBuilder other = (DiversifiedAggregationBuilder) obj;
        return Objects.equals(shardSize, other.shardSize)
            && Objects.equals(maxDocsPerValue, other.maxDocsPerValue)
            && Objects.equals(executionHint, other.executionHint);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
