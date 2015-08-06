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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

/**
 * Pseudo randomly generate a score for each {@link #score}.
 */
public class RandomScoreFunction extends ScoreFunction {

    private int originalSeed;
    private int saltedSeed;
    private final IndexFieldData<?> uidFieldData;

    /**
     * Default constructor. Only useful for constructing as a placeholder, but should not be used for actual scoring.
     */
    public RandomScoreFunction() {
        super(CombineFunction.MULT);
        uidFieldData = null;
    }

    /**
     * Creates a RandomScoreFunction.
     *
     * @param seed A seed for randomness
     * @param salt A value to salt the seed with, ideally unique to the running node/index
     * @param uidFieldData The field data for _uid to use for generating consistent random values for the same id
     */
    public RandomScoreFunction(int seed, int salt, IndexFieldData<?> uidFieldData) {
        super(CombineFunction.MULT);
        this.originalSeed = seed;
        this.saltedSeed = seed ^ salt;
        this.uidFieldData = uidFieldData;
        if (uidFieldData == null) throw new NullPointerException("uid missing");
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        AtomicFieldData leafData = uidFieldData.load(ctx);
        final SortedBinaryDocValues uidByteData = leafData.getBytesValues();
        if (uidByteData == null) throw new NullPointerException("failed to get uid byte data");

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) {
                uidByteData.setDocument(docId);
                int hash = StringHelper.murmurhash3_x86_32(uidByteData.valueAt(0), saltedSeed);
                return (hash & 0x00FFFFFF) / (float)(1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) {
                return Explanation.match(
                        CombineFunction.toFloat(score(docId, subQueryScore.getValue())),
                        "random score function (seed: " + originalSeed + ")");
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}
