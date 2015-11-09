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

    private Integer seed;

    public RandomScoreFunctionBuilder() {
    }

    @Override
    public String getName() {
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
    protected RandomScoreFunctionBuilder doReadFrom(StreamInput in) throws IOException {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
        randomScoreFunctionBuilder.seed = in.readInt();
        return randomScoreFunctionBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeInt(seed);
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
        final MappedFieldType fieldType = context.getMapperService().smartNameFieldType("_uid");
        if (fieldType == null) {
            // mapper could be null if we are on a shard with no docs yet, so this won't actually be used
            return new RandomScoreFunction();
        }
        //TODO find a way to not get the shard_id from the current search context? make it available in QueryShardContext?
        //this currently causes NPE in FunctionScoreQueryBuilderTests#testToQuery
        final ShardId shardId = SearchContext.current().indexShard().shardId();
        final int salt = (context.index().name().hashCode() << 10) | shardId.id();
        final IndexFieldData<?> uidFieldData = context.getForField(fieldType);
        return new RandomScoreFunction(this.seed == null ? hash(context.nowInMillis()) : seed, salt, uidFieldData);
    }

    private static int hash(long value) {
        return Long.hashCode(value);
    }
}
