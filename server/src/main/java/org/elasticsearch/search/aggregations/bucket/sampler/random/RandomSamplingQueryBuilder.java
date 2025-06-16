/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplingQuery.checkProbabilityRange;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RandomSamplingQueryBuilder extends AbstractQueryBuilder<RandomSamplingQueryBuilder> {

    public static final String NAME = "random_sampling";
    static final ParseField PROBABILITY = new ParseField("query");
    static final ParseField SEED = new ParseField("seed");
    static final ParseField HASH = new ParseField("hash");

    private final double probability;
    private int seed = Randomness.get().nextInt();
    private int hash = 0;

    public RandomSamplingQueryBuilder(double probability) {
        checkProbabilityRange(probability);
        this.probability = probability;
    }

    public RandomSamplingQueryBuilder seed(int seed) {
        checkProbabilityRange(probability);
        this.seed = seed;
        return this;
    }

    public RandomSamplingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.probability = in.readDouble();
        this.seed = in.readInt();
        this.hash = in.readInt();
    }

    public RandomSamplingQueryBuilder hash(Integer hash) {
        this.hash = hash;
        return this;
    }

    public double probability() {
        return probability;
    }

    public int seed() {
        return seed;
    }

    public int hash() {
        return hash;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(probability);
        out.writeInt(seed);
        out.writeInt(hash);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.field(SEED.getPreferredName(), seed);
        builder.field(HASH.getPreferredName(), hash);
        builder.endObject();
    }

    private static final ConstructingObjectParser<RandomSamplingQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            var randomSamplingQueryBuilder = new RandomSamplingQueryBuilder((double) args[0]);
            if (args[1] != null) {
                randomSamplingQueryBuilder.seed((int) args[1]);
            }
            if (args[2] != null) {
                randomSamplingQueryBuilder.hash((int) args[2]);
            }
            return randomSamplingQueryBuilder;
        }
    );

    static {
        PARSER.declareDouble(constructorArg(), PROBABILITY);
        PARSER.declareInt(optionalConstructorArg(), SEED);
        PARSER.declareInt(optionalConstructorArg(), HASH);
    }

    public static RandomSamplingQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return new RandomSamplingQuery(probability, seed, hash);
    }

    @Override
    protected boolean doEquals(RandomSamplingQueryBuilder other) {
        return probability == other.probability && seed == other.seed && hash == other.hash;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(probability, seed, hash);
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * The minimal version of the recipient this object can be sent to
     */
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.RANDOM_SAMPLER_QUERY_BUILDER;
    }
}
