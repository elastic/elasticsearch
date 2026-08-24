/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A {@link Similarity} wrapper that prevents {@code computeNorm} from returning zero for
 * non-empty fields. Lucene reserves norm {@code 0} to mean "field absent"; any similarity
 * that returns {@code 0} for a field with at least one token causes Lucene to throw an
 * {@link IllegalStateException} from {@code IndexingChain.PerField#finish}. That exception
 * escapes the {@code processDocument} finally-block before
 * {@code StoredFieldsConsumer.finishDocument} runs, leaving a stored-fields frame open and
 * desynchronising the writer's doc count. The segment then fails to flush with
 * {@code "Wrote N docs, finish called with numDocs=M"}, corrupting the shard.
 *
 * <p>This wrapper clamps a zero return value to {@code 1}, the smallest valid norm encoding
 * (equivalent to the shortest possible field length), so that the document is accepted and
 * the flush completes normally. It is applied to every {@link Similarity} registered in
 * {@link SimilarityService}, including built-in ones, so that analysis chains which produce
 * only overlap tokens ({@code positionIncrement == 0}) for some input cannot corrupt a shard.
 */
// package-private; exposed for testing via NonZeroNormSimilarityTests
final class NonZeroNormSimilarity extends Similarity {

    private final Similarity in;

    NonZeroNormSimilarity(Similarity in) {
        this.in = in;
    }

    Similarity getDelegate() {
        return in;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
        final long norm = in.computeNorm(state);
        // norm == 0 is reserved for "field absent" in Lucene's norm encoding.
        // Lucene only calls computeNorm when the field has tokens (length > 0), so a zero
        // return here always indicates a similarity that cannot represent the effective field
        // length (e.g. BM25 with discountOverlaps=true on a field whose only tokens all have
        // positionIncrement == 0). Clamp to 1 rather than letting Lucene throw.
        return norm == 0 ? 1L : norm;
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return in.scorer(boost, collectionStats, termStats);
    }
}
