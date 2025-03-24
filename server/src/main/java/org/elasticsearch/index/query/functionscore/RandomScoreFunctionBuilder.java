/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a random score for the matched documents
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder<RandomScoreFunctionBuilder> {

    public static final String NAME = "random_score";
    private String field;
    private Integer seed;

    public RandomScoreFunctionBuilder() {}

    /**
     * Read from a stream.
     */
    public RandomScoreFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            seed = in.readInt();
        }
        field = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (seed != null) {
            out.writeBoolean(true);
            out.writeInt(seed);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(field);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Sets the seed based on which the random number will be generated. Using the same seed is guaranteed to generate the same
     * random number for a specific doc.
     *
     * @param seed The seed.
     */
    public RandomScoreFunctionBuilder seed(int seed) {
        this.seed = seed;
        return this;
    }

    /**
     * seed variant taking a long value.
     * @see #seed(int)
     */
    public RandomScoreFunctionBuilder seed(long seed) {
        this.seed = hash(seed);
        return this;
    }

    /**
     * seed variant taking a String value.
     * @see #seed(int)
     */
    public RandomScoreFunctionBuilder seed(String seed) {
        if (seed == null) {
            throw new IllegalArgumentException("random_score function: seed must not be null");
        }
        this.seed = seed.hashCode();
        return this;
    }

    public Integer getSeed() {
        return seed;
    }

    /**
     * Set the field to be used for random number generation. This parameter is compulsory
     * when a {@link #seed(int) seed} is set and ignored otherwise. Note that documents that
     * have the same value for a field will get the same score.
     */
    public RandomScoreFunctionBuilder setField(String field) {
        this.field = field;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (seed != null) {
            builder.field("seed", seed);
        }
        if (field != null) {
            builder.field("field", field);
        }
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RandomScoreFunctionBuilder functionBuilder) {
        return Objects.equals(this.seed, functionBuilder.seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.seed);
    }

    @Override
    protected ScoreFunction doToFunction(SearchExecutionContext context) {
        final int salt = (context.index().getName().hashCode() << 10) | context.getShardId();
        if (seed == null) {
            // DocID-based random score generation
            return new RandomScoreFunction(hash(context.nowInMillis()), salt, null);
        } else {
            final String fieldName = Objects.requireNonNullElse(field, SeqNoFieldMapper.NAME);
            if (context.isFieldMapped(fieldName) == false) {
                if (context.hasMappings() == false) {
                    // no mappings: the index is empty anyway
                    return new RandomScoreFunction(hash(context.nowInMillis()), salt, null);
                }
                throw new IllegalArgumentException(
                    "Field [" + field + "] is not mapped on [" + context.index() + "] and cannot be used as a source of random numbers."
                );
            }
            int seed = this.seed == null ? hash(context.nowInMillis()) : this.seed;
            return new RandomScoreFunction(
                seed,
                salt,
                context.getForField(context.getFieldType(fieldName), MappedFieldType.FielddataOperation.SEARCH)
            );
        }
    }

    private static int hash(long value) {
        return Long.hashCode(value);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    public static RandomScoreFunctionBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("seed".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (parser.numberType() == XContentParser.NumberType.INT) {
                            randomScoreFunctionBuilder.seed(parser.intValue());
                        } else if (parser.numberType() == XContentParser.NumberType.LONG) {
                            randomScoreFunctionBuilder.seed(parser.longValue());
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "random_score seed must be an int, long or string, not '" + token.toString() + "'"
                            );
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        randomScoreFunctionBuilder.seed(parser.text());
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "random_score seed must be an int/long or string, not '" + token.toString() + "'"
                        );
                    }
                } else if ("field".equals(currentFieldName)) {
                    randomScoreFunctionBuilder.setField(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            }
        }
        return randomScoreFunctionBuilder;
    }

}
