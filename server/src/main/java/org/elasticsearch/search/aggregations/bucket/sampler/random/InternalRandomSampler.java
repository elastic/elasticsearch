/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class InternalRandomSampler extends InternalSingleBucketAggregation implements Sampler {
    public static final String NAME = "mapped_random_sampler";
    public static final String PARSER_NAME = "random_sampler";

    private final int seed;

    InternalRandomSampler(String name, long docCount, int seed, InternalAggregations subAggregations, Map<String, Object> metadata) {
        super(name, docCount, subAggregations, metadata);
        this.seed = seed;
    }

    /**
     * Read from a stream.
     */
    public InternalRandomSampler(StreamInput in) throws IOException {
        super(in);
        this.seed = in.readInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeInt(seed);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getType() {
        return PARSER_NAME;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalRandomSampler(name, docCount, seed, subAggregations, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RandomSamplerAggregationBuilder.SEED.getPreferredName(), seed);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        getAggregations().toXContentInternal(builder, params);
        return builder;
    }
}
