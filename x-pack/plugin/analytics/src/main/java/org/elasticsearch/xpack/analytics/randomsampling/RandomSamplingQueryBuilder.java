/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.randomsampling;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that randomly matches documents with a user-provided probability.  May
 * optionally include a seed so that matches are deterministic
 */
public class RandomSamplingQueryBuilder extends AbstractQueryBuilder<RandomSamplingQueryBuilder> {
    public static final String NAME = "random_sample";
    private static final ParseField PROBABILITY = new ParseField("probability");
    private static final ParseField SEED = new ParseField("seed");

    private double p = 0.5;
    private Integer seed = null;

    public RandomSamplingQueryBuilder(double probability) {
        this(probability, null);
    }

    public RandomSamplingQueryBuilder(double probability, Integer seed) {
        this.p = validateProbability(probability);
        this.seed = seed;
    }

    private RandomSamplingQueryBuilder() {
    }

    /**
     * Read from a stream.
     */
    public RandomSamplingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        p = in.readDouble();
        seed = in.readOptionalInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(p);
        out.writeOptionalInt(seed);
    }

    public double getProbability() {
        return p;
    }

    public void setProbability(double p) {
        this.p = validateProbability(p);
    }

    public Integer getSeed() {
        return seed;
    }

    public void setSeed(Integer seed) {
        this.seed = seed;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.field(PROBABILITY.getPreferredName(), p);
        if (seed != null) {
            builder.field(SEED.getPreferredName(), seed);
        }
        builder.endObject();
    }

    public static final ObjectParser<RandomSamplingQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RandomSamplingQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
        PARSER.declareDouble(RandomSamplingQueryBuilder::setProbability, PROBABILITY);
        PARSER.declareInt(RandomSamplingQueryBuilder::setSeed, SEED);
    }

    @Override
    protected final Query doToQuery(QueryShardContext context) {
        return new RandomSamplingQuery(p, Objects.requireNonNullElseGet(seed, () -> Long.hashCode(context.nowInMillis())), seed == null);
    }

    @Override
    protected boolean doEquals(RandomSamplingQueryBuilder other) {
        return Objects.equals(p, other.p)
            && Objects.equals(seed, other.seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(p, seed);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private double validateProbability(double p) {
        if (p <= 0.0) {
            throw new IllegalArgumentException("[" + PROBABILITY.getPreferredName() + "] cannot be less than or equal to 0.0.");
        }
        if (p >= 1.0) {
            throw new IllegalArgumentException("[" + PROBABILITY.getPreferredName() + "] cannot be greater than or equal to 1.0.");
        }
        return p;
    }
}
