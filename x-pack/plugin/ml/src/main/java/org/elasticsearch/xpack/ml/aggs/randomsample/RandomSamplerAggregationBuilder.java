/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.randomsample;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RandomSamplerAggregationBuilder extends AbstractAggregationBuilder<RandomSamplerAggregationBuilder> {

    public static final String NAME = "random_sampler";

    static final ParseField PROBABILITY = new ParseField("probability");
    static final ParseField SEED = new ParseField("seed");

    public static final ObjectParser<RandomSamplerAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        RandomSamplerAggregationBuilder.NAME,
        RandomSamplerAggregationBuilder::new
    );
    static {
        PARSER.declareInt(RandomSamplerAggregationBuilder::setSeed, SEED);
        PARSER.declareDouble(RandomSamplerAggregationBuilder::setProbability, PROBABILITY);
    }

    public static RandomSamplerAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new RandomSamplerAggregationBuilder(aggregationName), null);
    }

    private boolean setSeed = false;
    private int seed = Randomness.get().nextInt();
    private double p = 0.1;

    RandomSamplerAggregationBuilder(String name) {
        super(name);
    }

    public RandomSamplerAggregationBuilder setProbability(double probability) {
        if (probability <= 0 || probability >= 1) {
            throw new IllegalArgumentException("[probability] must be between 0 and 1, exclusive");
        }
        this.p = probability;
        return this;
    }

    public RandomSamplerAggregationBuilder setSeed(int seed) {
        this.seed = seed;
        this.setSeed = true;
        return this;
    }

    public RandomSamplerAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.p = in.readDouble();
        this.seed = in.readInt();
        this.setSeed = in.readBoolean();
    }

    protected RandomSamplerAggregationBuilder(
        RandomSamplerAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.p = clone.p;
        this.seed = clone.seed;
        this.setSeed = clone.setSeed;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(p);
        out.writeInt(seed);
        out.writeBoolean(setSeed);
    }

    void recursivelyFlattenAggs(Collection<AggregationBuilder> builders, List<AggregationBuilder> flattened) {
        if (builders == null || builders.isEmpty()) {
            return;
        }
        flattened.addAll(builders);
        builders.forEach(b -> recursivelyFlattenAggs(b.getSubAggregations(), flattened));
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
        List<AggregationBuilder> flattenBuilders = new ArrayList<>();
        recursivelyFlattenAggs(subfactoriesBuilder.getAggregatorFactories(), flattenBuilders);
        for (AggregationBuilder builder : flattenBuilders) {
            if (builder instanceof CardinalityAggregationBuilder || builder instanceof NestedAggregationBuilder) {
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
        }
        return new RandomSamplerAggregatorFactory(name, seed, p, context, parent, subfactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROBABILITY.getPreferredName(), p);
        if (setSeed) {
            builder.field(SEED.getPreferredName(), seed);
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
    public int hashCode() {
        if (setSeed) {
            return Objects.hash(super.hashCode(), p, seed);
        } else {
            return Objects.hash(super.hashCode(), p);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        RandomSamplerAggregationBuilder other = (RandomSamplerAggregationBuilder) obj;
        return Objects.equals(p, other.p) && (setSeed == false || Objects.equals(seed, other.seed));
    }
}
