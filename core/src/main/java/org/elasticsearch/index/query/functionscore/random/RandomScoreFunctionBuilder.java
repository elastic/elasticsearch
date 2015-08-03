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
package org.elasticsearch.index.query.functionscore.random;

import com.google.common.primitives.Longs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a random score for the matched documents
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder<RandomScoreFunctionBuilder> {

    static final RandomScoreFunctionBuilder PROTOTYPE = new RandomScoreFunctionBuilder();

    public static final int DEFAULT_SEED = -1;

    private int seed = DEFAULT_SEED;

    public RandomScoreFunctionBuilder() {
    }

    @Override
    public String getWriteableName() {
        return RandomScoreFunctionParser.NAMES[0];
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
     * @see {@link #seed(int)}
     */
    public RandomScoreFunctionBuilder seed(long seed) {
        this.seed = Longs.hashCode(seed);
        return this;
    }

    /**
     * seed variant taking a String value.
     * @see {@link #seed(int)}
     */
    public RandomScoreFunctionBuilder seed(String seed) {
        this.seed = seed.hashCode();
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        builder.field("seed", seed);
        builder.endObject();
    }

    @Override
    protected ScoreFunction doScoreFunction(QueryShardContext context) throws IOException {
        final MappedFieldType fieldType = context.mapperService().smartNameFieldType("_uid");
        if (fieldType == null) {
            // mapper could be null if we are on a shard with no docs yet, so this won't actually be used
            return new RandomScoreFunction();
        }

        if (seed == DEFAULT_SEED) {
            seed = Longs.hashCode(context.nowInMillis());
        }
        //NO COMMIT we need to make indexShard available in our parseContext test!
        final ShardId shardId = SearchContext.current().indexShard().shardId();
        final int salt = (shardId.index().name().hashCode() << 10) | shardId.id();
        final IndexFieldData<?> uidFieldData = context.fieldData().getForField(fieldType);

        return new RandomScoreFunction(seed, salt, uidFieldData);
    }

    @Override
    protected RandomScoreFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
        randomScoreFunctionBuilder.seed = in.readVInt();
        return randomScoreFunctionBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(seed);
    }

    @Override
    protected boolean doEquals(RandomScoreFunctionBuilder other) {
        return Objects.equals(seed, other.seed);
    }
}