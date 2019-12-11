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
package org.elasticsearch.index.query.functionscore;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a random score for the matched documents
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder<RandomScoreFunctionBuilder> {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(RandomScoreFunctionBuilder.class));

    public static final String NAME = "random_score";
    private String field;
    private Integer seed;

    public RandomScoreFunctionBuilder() {
    }

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

    /**
     * Get the field to use for random number generation.
     * @see #setField(String)
     */
    public String getField() {
        return field;
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
    protected ScoreFunction doToFunction(QueryShardContext context) {
        final int salt = (context.index().getName().hashCode() << 10) | context.getShardId();
        if (seed == null) {
            // DocID-based random score generation
            return new RandomScoreFunction(hash(context.nowInMillis()), salt, null);
        } else {
            final MappedFieldType fieldType;
            if (field != null) {
                fieldType = context.getMapperService().fullName(field);
            } else {
                deprecationLogger.deprecated(
                        "As of version 7.0 Elasticsearch will require that a [field] parameter is provided when a [seed] is set");
                fieldType = context.getMapperService().fullName(IdFieldMapper.NAME);
            }
            if (fieldType == null) {
                if (context.getMapperService().documentMapper() == null) {
                    // no mappings: the index is empty anyway
                    return new RandomScoreFunction(hash(context.nowInMillis()), salt, null);
                }
                throw new IllegalArgumentException("Field [" + field + "] is not mapped on [" + context.index() +
                        "] and cannot be used as a source of random numbers.");
            }
            int seed;
            if (this.seed != null) {
                seed = this.seed;
            } else {
                seed = hash(context.nowInMillis());
            }
            return new RandomScoreFunction(seed, salt, context.getForField(fieldType));
        }
    }

    private static int hash(long value) {
        return Long.hashCode(value);
    }

    public static RandomScoreFunctionBuilder fromXContent(XContentParser parser)
            throws IOException, ParsingException {
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
                            throw new ParsingException(parser.getTokenLocation(), "random_score seed must be an int, long or string, not '"
                                    + token.toString() + "'");
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        randomScoreFunctionBuilder.seed(parser.text());
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "random_score seed must be an int/long or string, not '"
                                + token.toString() + "'");
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
