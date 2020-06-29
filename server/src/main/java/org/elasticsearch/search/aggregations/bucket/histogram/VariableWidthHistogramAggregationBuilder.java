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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class VariableWidthHistogramAggregationBuilder
    extends ValuesSourceAggregationBuilder<VariableWidthHistogramAggregationBuilder> {

    public static final String NAME = "variable_width_histogram";

    private static final ParseField NUM_BUCKETS_FIELD = new ParseField("buckets");

    private static ParseField INITIAL_BUFFER_FIELD = new ParseField("initial_buffer");

    private static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");

    public static final ObjectParser<VariableWidthHistogramAggregationBuilder, String> PARSER =
        ObjectParser.fromBuilder(NAME, VariableWidthHistogramAggregationBuilder::new);
    static{
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setNumBuckets, NUM_BUCKETS_FIELD);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setShardSize, SHARD_SIZE_FIELD);
        PARSER.declareInt(VariableWidthHistogramAggregationBuilder::setInitialBuffer, INITIAL_BUFFER_FIELD);
    }

    private int numBuckets = 10;
    private int shardSize = numBuckets * 50;
    private int initialBuffer = Math.min(10 * this.shardSize, 50000);

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        VariableWidthHistogramAggregatorFactory.registerAggregators(builder);
    }
    /** Create a new builder with the given name. */
    public VariableWidthHistogramAggregationBuilder(String name) {
        super(name);
    }

    /** Read in object data from a stream, for internal use only. */
    public VariableWidthHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        numBuckets = in.readVInt();
    }

    protected VariableWidthHistogramAggregationBuilder(VariableWidthHistogramAggregationBuilder clone,
                                                       AggregatorFactories.Builder factoriesBuilder,
                                                       Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.numBuckets = clone.numBuckets;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    public VariableWidthHistogramAggregationBuilder setNumBuckets(int numBuckets){
        if (numBuckets <= 0) {
            throw new IllegalArgumentException(NUM_BUCKETS_FIELD.getPreferredName() + " must be greater than 0 for ["
                + name + "]");
        } else if (numBuckets > 50000){
            throw new IllegalArgumentException(NUM_BUCKETS_FIELD.getPreferredName() + " must not be greater than 50,000 for ["
                + name + "]");
        }
        this.numBuckets = numBuckets;
        return this;
    }

    public VariableWidthHistogramAggregationBuilder setShardSize(int shardSize){
        if (shardSize < numBuckets) {
            throw new IllegalArgumentException(SHARD_SIZE_FIELD.getPreferredName() + " must not be less than "
                + NUM_BUCKETS_FIELD.getPreferredName() + " for [" + name + "]");
        }
        this.shardSize = shardSize;
        return this;
    }

    public VariableWidthHistogramAggregationBuilder setInitialBuffer(int initialBuffer){
        if (initialBuffer < numBuckets) {
            // If numBuckets buckets are being returned, then at least that many must be stored in memory
            throw new IllegalArgumentException(INITIAL_BUFFER_FIELD.getPreferredName() + " must be greater than numBuckets "
                + NUM_BUCKETS_FIELD.getPreferredName() + " for [" + name + "]");

        }
        this.initialBuffer = initialBuffer;
        return this;
    }

    public int getNumBuckets(){ return numBuckets; }

    public int getShardSize(){ return shardSize; }

    public int getInitialBuffer(){ return initialBuffer; }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new VariableWidthHistogramAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(numBuckets);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {

        Settings settings = queryShardContext.getIndexSettings().getNodeSettings();
        int maxBuckets = MultiBucketConsumerService.MAX_BUCKET_SETTING.get(settings);
        if (numBuckets > maxBuckets) {
            throw new IllegalArgumentException(NUM_BUCKETS_FIELD.getPreferredName()+
                " must be less than " + maxBuckets);
        }
        return new VariableWidthHistogramAggregatorFactory(name, config, numBuckets, shardSize, initialBuffer,
            queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(NUM_BUCKETS_FIELD.getPreferredName(), numBuckets);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numBuckets, shardSize, initialBuffer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        VariableWidthHistogramAggregationBuilder other = (VariableWidthHistogramAggregationBuilder) obj;
        return Objects.equals(numBuckets, other.numBuckets)
            && Objects.equals(shardSize, other.shardSize)
            && Objects.equals(initialBuffer, other.initialBuffer);
    }

    @Override
    public String getType() { return NAME; }
}
