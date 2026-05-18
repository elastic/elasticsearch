/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.lucene.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Similarity that behaves like {@link BM25Similarity} while also applying
 * the k1+1 factor to the numerator of the scoring formula
 *
 * @see BM25Similarity
 *
 * @deprecated {@link BM25Similarity} should be used instead
 */
@Deprecated
public final class LegacyBM25Similarity extends Similarity {

    private final BM25Similarity bm25Similarity;

    /** BM25 with these default values:
     * <ul>
     *   <li>{@code k1 = 1.2}</li>
     *   <li>{@code b = 0.75}</li>
     * </ul>
     */
    public LegacyBM25Similarity() {
        this(new BM25Similarity());
    }

    /**
     * BM25 with the supplied parameter values.
     * @param k1 Controls non-linear term frequency normalization (saturation).
     * @param b Controls to what degree document length normalizes tf values.
     * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is
     *         not within the range {@code [0..1]}
     */
    public LegacyBM25Similarity(float k1, float b, boolean discountOverlaps) {
        this(new BM25Similarity(k1, b, discountOverlaps));
    }

    private LegacyBM25Similarity(BM25Similarity bm25Similarity) {
        super(bm25Similarity.getDiscountOverlaps());
        this.bm25Similarity = bm25Similarity;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        return bm25Similarity.computeNorm(state);
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return bm25Similarity.scorer(boost * (1 + bm25Similarity.getK1()), collectionStats, termStats);
    }

    /**
     * Returns the <code>k1</code> parameter
     */
    public float getK1() {
        return bm25Similarity.getK1();
    }

    /**
     * Returns the <code>b</code> parameter
     */
    public float getB() {
        return bm25Similarity.getB();
    }

    @Override
    public String toString() {
        return bm25Similarity.toString();
    }
}
