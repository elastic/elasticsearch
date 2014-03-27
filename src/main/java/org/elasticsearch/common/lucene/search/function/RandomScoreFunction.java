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

import com.carrotsearch.hppc.hash.MurmurHash3;
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
        exp.setDescription("random score function (seed: " + prng.seed + ")");
        exp.addDetail(subQueryExpl);
        return exp;
    }

    /**
     * Random score generator that always returns the same score to the same documents,
     * provided that it has been instantiated with the same seed.
     */
    static class PRNG {

        final long seed;

        PRNG(long seed) {
            this.seed = seed;
        }

        public float random(int doc) {
            // TODO: is it equally fair to all documents?
            long rand = MurmurHash3.hash(seed ^ (long) doc);
            rand &= (1 << 31) - 1;
            return (float) rand / Integer.MAX_VALUE;
        }

    }
}
