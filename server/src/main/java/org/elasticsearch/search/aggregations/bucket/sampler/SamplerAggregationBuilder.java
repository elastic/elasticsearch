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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SamplerAggregationBuilder extends AbstractAggregationBuilder<SamplerAggregationBuilder> {
    public static final String NAME = "sampler";

    public static final int DEFAULT_SHARD_SAMPLE_SIZE = 100;

    private int shardSize = DEFAULT_SHARD_SAMPLE_SIZE;

    public SamplerAggregationBuilder(String name) {
        super(name);
    }

    protected SamplerAggregationBuilder(SamplerAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.shardSize = clone.shardSize;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new SamplerAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public SamplerAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        shardSize = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(shardSize);
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public SamplerAggregationBuilder shardSize(int shardSize) {
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
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected SamplerAggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
            throws IOException {
        return new SamplerAggregatorFactory(name, shardSize, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SamplerAggregator.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        builder.endObject();
        return builder;
    }

    public static SamplerAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Integer shardSize = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SamplerAggregator.SHARD_SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    shardSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName);
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName);
            }
        }

        SamplerAggregationBuilder factory = new SamplerAggregationBuilder(aggregationName);
        if (shardSize != null) {
            factory.shardSize(shardSize);
        }
        return factory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shardSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        SamplerAggregationBuilder other = (SamplerAggregationBuilder) obj;
        return Objects.equals(shardSize, other.shardSize);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
