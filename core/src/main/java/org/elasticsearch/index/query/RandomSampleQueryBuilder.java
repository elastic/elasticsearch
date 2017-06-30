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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that randomly matches documents with a user-provided probability.  May
 * optionally include a seed so that matches are deterministic
 */
public class RandomSampleQueryBuilder extends AbstractQueryBuilder<RandomSampleQueryBuilder> {
    public static final String NAME = "random_sample";
    private static final ParseField PROBABILITY = new ParseField("probability");
    private static final ParseField SEED = new ParseField("seed");

    private double p = 0.5;
    private Long seed = null;

    RandomSampleQueryBuilder(double probability) {
        this(probability, null);
    }

    RandomSampleQueryBuilder(double probability, Long seed) {
        this.p = validateProbability(probability);
        this.seed = seed;
    }

    private RandomSampleQueryBuilder() {
    }

    /**
     * Read from a stream.
     */
    public RandomSampleQueryBuilder(StreamInput in) throws IOException {
        super(in);
        p = in.readDouble();
        seed = in.readOptionalLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(p);
        out.writeOptionalLong(seed);
    }

    public double getProbability() {
        return p;
    }

    public void setProbability(double p) {
        this.p = validateProbability(p);
    }

    public Long getSeed() {
        return seed;
    }

    public void setSeed(Long seed) {
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

    private static final ObjectParser<RandomSampleQueryBuilder, QueryParseContext> PARSER
        = new ObjectParser<>(NAME, RandomSampleQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
        PARSER.declareDouble(RandomSampleQueryBuilder::setProbability, PROBABILITY);
        PARSER.declareLong(RandomSampleQueryBuilder::setSeed, SEED);
    }

    public static RandomSampleQueryBuilder fromXContent(QueryParseContext context) {
        try {
            return PARSER.apply(context.parser(), context);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(context.parser().getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        return new RandomSampleQuery(p, seed);
    }

    @Override
    protected boolean doEquals(RandomSampleQueryBuilder other) {
        return Objects.equals(p, other.p) &&
            Objects.equals(seed, other.seed);
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
