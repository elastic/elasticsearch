/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 *
 */
public class RandomScoreFunction extends ScoreFunction {

    private final PRNG prng;
    private int docBase;

    public RandomScoreFunction(long seed) {
        super(CombineFunction.MULT);
        this.prng = new PRNG(seed);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.docBase = context.docBase;
    }

    @Override
    public double score(int docId, float subQueryScore) {
        return prng.random(docBase + docId);
    }

    @Override
    public Explanation explainScore(int docId, Explanation subQueryExpl) {
        Explanation exp = new Explanation();
        exp.setDescription("random score function (seed: " + prng.originalSeed + ")");
        exp.addDetail(subQueryExpl);
        return exp;
    }

    /**
     * Algorithm largely based on {@link java.util.Random} except this one is not
     * thread safe and it incorporates the doc id on next();
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

        public float random(int doc) {
            if (doc == 0) {
                doc = 0xCAFEBAB;
            }

            long rand = doc;
            rand |= rand << 32;
            rand ^= rand;
            return nextFloat(rand);
        }

        public float nextFloat(long rand) {
            seed = (seed * multiplier + addend) & mask;
            rand ^= seed;
            double result = rand / (double)(1L << 54);
            return (float) result;
        }

    }
}
