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
package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;

/**
 * Pseudo randomly generate a score for each {@link #score}.
 */
public class RandomScoreFunction extends ScoreFunction {

    private final PRNG prng;

    public RandomScoreFunction(long seed) {
        super(CombineFunction.MULT);
        this.prng = new PRNG(seed);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        // intentionally does nothing
    }

    @Override
    public double score(int docId, float subQueryScore) {
        return prng.nextFloat();
    }

    @Override
    public Explanation explainScore(int docId, Explanation subQueryExpl) {
        Explanation exp = new Explanation();
        exp.setDescription("random score function (seed: " + prng.originalSeed + ")");
        exp.addDetail(subQueryExpl);
        return exp;
    }

    /**
     * A non thread-safe PRNG
     */
    static class PRNG {

        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;

        final long originalSeed;
        long seed;

        PRNG(long seed) {
            this.originalSeed = seed;
            this.seed = (seed ^ multiplier) & mask;
        }

        public float nextFloat() {
            seed = (seed * multiplier + addend) & mask;
            return seed / (float)(1 << 24);
        }

    }
}
