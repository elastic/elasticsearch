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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;

/**
 * A function that computes a random score for the matched documents
 */
public class RandomScoreFunctionBuilder extends ScoreFunctionBuilder {

    private Object seed = null;

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
     * @see {@link #seed(int)}
     */
    public RandomScoreFunctionBuilder seed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * seed variant taking a String value.
     * @see {@link #seed(int)}
     */
    public RandomScoreFunctionBuilder seed(String seed) {
        this.seed = seed;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (seed instanceof Number) {
            builder.field("seed", ((Number)seed).longValue());
        } else if (seed != null) {
            builder.field("seed", seed.toString());
        }
        builder.endObject();
    }

}