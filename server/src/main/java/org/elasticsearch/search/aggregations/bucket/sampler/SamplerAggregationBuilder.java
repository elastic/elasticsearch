/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;

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
    protected SamplerAggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        return new SamplerAggregatorFactory(name, shardSize, context, parent, subFactoriesBuilder, metadata);
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
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName
                );
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
