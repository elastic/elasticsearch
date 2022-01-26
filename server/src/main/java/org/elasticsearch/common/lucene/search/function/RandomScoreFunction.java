/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene.search.function;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Objects;

/**
 * Pseudo randomly generate a score for each {@link LeafScoreFunction#score}.
 */
public class RandomScoreFunction extends ScoreFunction {

    private final int originalSeed;
    private final int saltedSeed;
    private final IndexFieldData<?> fieldData;

    /**
     * Creates a RandomScoreFunction.
     *
     * @param seed A seed for randomness
     * @param salt A value to salt the seed with, ideally unique to the running node/index
     * @param uidFieldData The field data for _uid to use for generating consistent random values for the same id
     */
    public RandomScoreFunction(int seed, int salt, IndexFieldData<?> uidFieldData) {
        super(CombineFunction.MULTIPLY);
        this.originalSeed = seed;
        this.saltedSeed = BitMixer.mix(seed, salt);
        this.fieldData = uidFieldData;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final SortedBinaryDocValues values;
        if (fieldData != null) {
            LeafFieldData leafData = fieldData.load(ctx);
            values = leafData.getBytesValues();
            if (values == null) throw new NullPointerException("failed to get fielddata");
        } else {
            values = null;
        }

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                int hash;
                if (values == null) {
                    hash = BitMixer.mix(ctx.docBase + docId, saltedSeed);
                } else if (values.advanceExact(docId)) {
                    hash = StringHelper.murmurhash3_x86_32(values.nextValue(), saltedSeed);
                } else {
                    // field has no value
                    hash = saltedSeed;
                }
                return (hash & 0x00FFFFFF) / (float) (1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                String field = fieldData == null ? null : fieldData.getFieldName();
                return Explanation.match(
                    (float) score(docId, subQueryScore.getValue().floatValue()),
                    "random score function (seed: " + originalSeed + ", field: " + field + ")"
                );
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        RandomScoreFunction randomScoreFunction = (RandomScoreFunction) other;
        return this.originalSeed == randomScoreFunction.originalSeed && this.saltedSeed == randomScoreFunction.saltedSeed;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalSeed, saltedSeed);
    }
}
