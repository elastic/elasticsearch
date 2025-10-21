/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.AggregatorsReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InternalRandomSampler extends InternalSingleBucketAggregation {
    public static final String NAME = "mapped_random_sampler";
    public static final String PARSER_NAME = "random_sampler";

    private final int seed;
    private final Integer shardSeed;
    private final double probability;

    InternalRandomSampler(
        String name,
        long docCount,
        int seed,
        Integer shardSeed,
        double probability,
        InternalAggregations subAggregations,
        Map<String, Object> metadata
    ) {
        super(name, docCount, subAggregations, metadata);
        this.seed = seed;
        this.shardSeed = shardSeed;
        this.probability = probability;
    }

    /**
     * Read from a stream.
     */
    public InternalRandomSampler(StreamInput in) throws IOException {
        super(in);
        this.seed = in.readInt();
        this.probability = in.readDouble();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.shardSeed = in.readOptionalInt();
        } else {
            this.shardSeed = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeInt(seed);
        out.writeDouble(probability);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalInt(shardSeed);
        }
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
        return new InternalRandomSampler(name, docCount, seed, shardSeed, probability, subAggregations, metadata);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            long docCount = 0L;
            final AggregatorsReducer subAggregatorReducer = new AggregatorsReducer(getAggregations(), reduceContext, size);

            @Override
            public void accept(InternalAggregation aggregation) {
                docCount += ((InternalSingleBucketAggregation) aggregation).getDocCount();
                subAggregatorReducer.accept(((InternalSingleBucketAggregation) aggregation).getAggregations());
            }

            @Override
            public InternalAggregation get() {
                InternalAggregations aggs = subAggregatorReducer.get();
                if (reduceContext.isFinalReduce() && aggs != null) {
                    SamplingContext context = buildContext();
                    final List<InternalAggregation> aaggregationList = aggs.asList();
                    final List<InternalAggregation> sampledAggregations = new ArrayList<>(aaggregationList.size());
                    for (InternalAggregation agg : aaggregationList) {
                        sampledAggregations.add(agg.finalizeSampling(context));
                    }
                    aggs = InternalAggregations.from(sampledAggregations);
                }
                return newAggregation(getName(), docCount, aggs);
            }

            @Override
            public void close() {
                Releasables.close(subAggregatorReducer);
            }
        };
    }

    public SamplingContext buildContext() {
        return new SamplingContext(probability, seed, shardSeed);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RandomSamplerAggregationBuilder.SEED.getPreferredName(), seed);
        if (shardSeed != null) {
            builder.field(RandomSamplerAggregationBuilder.SHARD_SEED.getPreferredName(), shardSeed);
        }
        builder.field(RandomSamplerAggregationBuilder.PROBABILITY.getPreferredName(), probability);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        getAggregations().toXContentInternal(builder, params);
        return builder;
    }
}
