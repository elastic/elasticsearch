/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.search;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.SmallFloat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.search.XCombinedFieldQuery.FieldAndWeight;

/**
 * Copy of {@link LeafSimScorer} that sums document's norms from multiple fields.
 *
 * TODO: this is temporarily copied from Lucene, remove once we update to Lucene 8.9.
 */
final class XMultiNormsLeafSimScorer {
    /**
     * Cache of decoded norms.
     */
    private static final float[] LENGTH_TABLE = new float[256];

    static {
        for (int i = 0; i < 256; i++) {
            LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
        }
    }

    private final SimScorer scorer;
    private final NumericDocValues norms;

    /**
     * Sole constructor: Score documents of {@code reader} with {@code scorer}.
     *
     */
    XMultiNormsLeafSimScorer(SimScorer scorer,
                             LeafReader reader,
                             Collection<FieldAndWeight> normFields,
                             boolean needsScores) throws IOException {
        this.scorer = Objects.requireNonNull(scorer);
        if (needsScores) {
            final List<NumericDocValues> normsList = new ArrayList<>();
            final List<Float> weightList = new ArrayList<>();
            for (FieldAndWeight field : normFields) {
                NumericDocValues norms = reader.getNormValues(field.field);
                if (norms != null) {
                    normsList.add(norms);
                    weightList.add(field.weight);
                }
            }
            if (normsList.isEmpty()) {
                norms = null;
            } else if (normsList.size() == 1) {
                norms = normsList.get(0);
            } else {
                final NumericDocValues[] normsArr = normsList.toArray(new NumericDocValues[0]);
                final float[] weightArr = new float[normsList.size()];
                for (int i = 0; i < weightList.size(); i++) {
                    weightArr[i] = weightList.get(i);
                }
                norms = new XMultiNormsLeafSimScorer.MultiFieldNormValues(normsArr, weightArr);
            }
        } else {
            norms = null;
        }
    }

    private long getNormValue(int doc) throws IOException {
        if (norms != null) {
            boolean found = norms.advanceExact(doc);
            assert found;
            return norms.longValue();
        } else {
            return 1L; // default norm
        }
    }

    /** Score the provided document assuming the given term document frequency.
     *  This method must be called on non-decreasing sequences of doc ids.
     *  @see SimScorer#score(float, long) */
    public float score(int doc, float freq) throws IOException {
        return scorer.score(freq, getNormValue(doc));
    }

    /** Explain the score for the provided document assuming the given term document frequency.
     *  This method must be called on non-decreasing sequences of doc ids.
     *  @see SimScorer#explain(Explanation, long) */
    public Explanation explain(int doc, Explanation freqExpl) throws IOException {
        return scorer.explain(freqExpl, getNormValue(doc));
    }

    private static class MultiFieldNormValues extends NumericDocValues {
        private final NumericDocValues[] normsArr;
        private final float[] weightArr;
        private long current;
        private int docID = -1;

        MultiFieldNormValues(NumericDocValues[] normsArr, float[] weightArr) {
            this.normsArr = normsArr;
            this.weightArr = weightArr;
        }

        @Override
        public long longValue() {
            return current;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            float normValue = 0;
            for (int i = 0; i < normsArr.length; i++) {
                boolean found = normsArr[i].advanceExact(target);
                assert found;
                normValue += weightArr[i] * LENGTH_TABLE[Byte.toUnsignedInt((byte) normsArr[i].longValue())];
            }
            current = SmallFloat.intToByte4(Math.round(normValue));
            return true;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }
    }
}
