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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a random score for the matched documents
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder<RandomScoreFunctionBuilder> {
    public static final String NAME = "random_score";
    public static final ParseField FUNCTION_NAME_FIELD = new ParseField(NAME);
    private Integer seed;

    public RandomScoreFunctionBuilder() {
    }

    /**
     * Read from a stream.
     */
    public RandomScoreFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        seed = in.readInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeInt(seed);
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

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (seed != null) {
            builder.field("seed", seed);
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
        final MappedFieldType fieldType = context.getMapperService().fullName("_uid");
        if (fieldType == null) {
            // mapper could be null if we are on a shard with no docs yet, so this won't actually be used
            return new RandomScoreFunction();
        }
        final int salt = (context.index().getName().hashCode() << 10) | getCurrentShardId();
        final IndexFieldData<?> uidFieldData = context.getForField(fieldType);
        return new RandomScoreFunction(this.seed == null ? hash(context.nowInMillis()) : seed, salt, uidFieldData);
    }

    /**
     * Get the current shard's id for the seed. Protected because this method doesn't work during certain unit tests and needs to be
     * replaced.
     */
    int getCurrentShardId() {
        return SearchContext.current().indexShard().shardId().id();
    }

    private static int hash(long value) {
        return Long.hashCode(value);
    }

    public static RandomScoreFunctionBuilder fromXContent(QueryParseContext parseContext)
            throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();
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
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query does not support [" + currentFieldName + "]");
                }
            }
        }
        return randomScoreFunctionBuilder;
    }

}
