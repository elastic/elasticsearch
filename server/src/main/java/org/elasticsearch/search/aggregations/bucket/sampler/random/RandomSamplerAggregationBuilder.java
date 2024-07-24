/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class RandomSamplerAggregationBuilder extends AbstractAggregationBuilder<RandomSamplerAggregationBuilder> {

    public static final String NAME = "random_sampler";

    static final ParseField PROBABILITY = new ParseField("probability");
    static final ParseField SEED = new ParseField("seed");
    static final ParseField SHARD_SEED = new ParseField("shard_seed");

    public static final ObjectParser<RandomSamplerAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        RandomSamplerAggregationBuilder.NAME,
        RandomSamplerAggregationBuilder::new
    );
    static {
        PARSER.declareInt(RandomSamplerAggregationBuilder::setSeed, SEED);
        PARSER.declareInt(RandomSamplerAggregationBuilder::setShardSeed, SHARD_SEED);
        PARSER.declareDouble(RandomSamplerAggregationBuilder::setProbability, PROBABILITY);
    }

    private int seed = Randomness.get().nextInt();
    private Integer shardSeed;
    private double p;

    public RandomSamplerAggregationBuilder(String name) {
        super(name);
    }

    public RandomSamplerAggregationBuilder setProbability(double probability) {
        if (probability <= 0) {
            throw new IllegalArgumentException("[probability] must be greater than 0.0, was [" + probability + "]");
        }
        if (probability > 0.5 && probability != 1.0) {
            throw new IllegalArgumentException("[probability] must be between 0.0 and 0.5 or exactly 1.0, was [" + probability + "]");
        }
        this.p = probability;
        return this;
    }

    public RandomSamplerAggregationBuilder setSeed(int seed) {
        this.seed = seed;
        return this;
    }

    public RandomSamplerAggregationBuilder setShardSeed(int shardSeed) {
        this.shardSeed = shardSeed;
        return this;
    }

    public RandomSamplerAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.p = in.readDouble();
        this.seed = in.readInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.shardSeed = in.readOptionalInt();
        }
    }

    protected RandomSamplerAggregationBuilder(
        RandomSamplerAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.p = clone.p;
        this.seed = clone.seed;
        this.shardSeed = clone.shardSeed;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(p);
        out.writeInt(seed);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalInt(shardSeed);
        }
    }

    static void recursivelyCheckSubAggs(Collection<AggregationBuilder> builders, Consumer<AggregationBuilder> aggregationCheck) {
        if (builders == null || builders.isEmpty()) {
            return;
        }
        for (AggregationBuilder b : builders) {
            aggregationCheck.accept(b);
            recursivelyCheckSubAggs(b.getSubAggregations(), aggregationCheck);
        }
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        if (parent != null) {
            throw new IllegalArgumentException("[random_sampler] aggregation [" + getName() + "] cannot have a parent aggregation");
        }
        if (subfactoriesBuilder.getAggregatorFactories().isEmpty()) {
            throw new IllegalArgumentException("[random_sampler] aggregation [" + getName() + "] must have sub-aggregations");
        }
        if (p == 0.0) {
            throw new IllegalArgumentException("[random_sampler] aggregation [" + getName() + "] must have [probability] set");
        }
        recursivelyCheckSubAggs(subfactoriesBuilder.getAggregatorFactories(), builder -> {
            // TODO add a method or interface to aggregation builder that defaults to false
            if (builder.supportsSampling() == false) {
                throw new IllegalArgumentException(
                    "[random_sampler] aggregation ["
                        + getName()
                        + "] does not support sampling ["
                        + builder.getType()
                        + "] aggregation ["
                        + builder.getName()
                        + "]"
                );
            }
        });
        return new RandomSamplerAggregatorFactory(name, seed, shardSeed, p, context, parent, subfactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROBABILITY.getPreferredName(), p);
        builder.field(SEED.getPreferredName(), seed);
        if (shardSeed != null) {
            builder.field(SHARD_SEED.getPreferredName(), shardSeed);
        }
        builder.endObject();
        return null;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RandomSamplerAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_2_0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), p, seed, shardSeed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        RandomSamplerAggregationBuilder other = (RandomSamplerAggregationBuilder) obj;
        return Objects.equals(p, other.p) && Objects.equals(seed, other.seed) && Objects.equals(shardSeed, other.shardSeed);
    }
}
