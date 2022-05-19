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
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InternalRandomSampler extends InternalSingleBucketAggregation implements Sampler {
    public static final String NAME = "mapped_random_sampler";
    public static final String PARSER_NAME = "random_sampler";

    private final int seed;
    private final double probability;

    InternalRandomSampler(
        String name,
        long docCount,
        int seed,
        double probability,
        InternalAggregations subAggregations,
        Map<String, Object> metadata
    ) {
        super(name, docCount, subAggregations, metadata);
        this.seed = seed;
        this.probability = probability;
    }

    /**
     * Read from a stream.
     */
    public InternalRandomSampler(StreamInput in) throws IOException {
        super(in);
        this.seed = in.readInt();
        this.probability = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeInt(seed);
        out.writeDouble(probability);
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
        return new InternalRandomSampler(name, docCount, seed, probability, subAggregations, metadata);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        long docCount = 0L;
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            docCount += ((InternalSingleBucketAggregation) aggregation).getDocCount();
            subAggregationsList.add(((InternalSingleBucketAggregation) aggregation).getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(subAggregationsList, reduceContext);
        if (reduceContext.isFinalReduce() && aggs != null) {
            SamplingContext context = buildContext();
            aggs = InternalAggregations.from(
                aggs.asList().stream().map(agg -> ((InternalAggregation) agg).finalizeSampling(context)).toList()
            );
        }

        return newAggregation(getName(), docCount, aggs);
    }

    public SamplingContext buildContext() {
        return new SamplingContext(probability, seed);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RandomSamplerAggregationBuilder.SEED.getPreferredName(), seed);
        builder.field(RandomSamplerAggregationBuilder.PROBABILITY.getPreferredName(), probability);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        getAggregations().toXContentInternal(builder, params);
        return builder;
    }
}
