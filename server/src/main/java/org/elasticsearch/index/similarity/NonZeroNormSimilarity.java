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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.TimeUnit;

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
 * the flush completes normally. In {@link SimilarityService} it is applied at the per-field
 * level (inside {@link SimilarityService.PerFieldSimilarity#get}) so that the outer
 * {@code PerFieldSimilarity} class name is preserved in Lucene's explain API (which uses
 * {@link Class#getSimpleName()} rather than {@link Object#toString()}).
 */
// package-private; exposed for testing via NonZeroNormSimilarityTests
final class NonZeroNormSimilarity extends Similarity {

    private static final Logger logger = LogManager.getLogger(NonZeroNormSimilarity.class);

    private static final long WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    /** Last time a zero-norm warning was emitted, in {@link System#nanoTime()} units. */
    private static volatile long lastWarnNanos = 0;

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
        if (norm == 0) {
            // norm == 0 is reserved for "field absent" in Lucene's norm encoding.
            // Lucene only calls computeNorm when the field has tokens (length > 0), so a zero
            // return here always indicates a similarity that cannot represent the effective field
            // length (e.g. BM25 with discountOverlaps=true on a field whose only tokens all have
            // positionIncrement == 0). Log a warning (rate-limited to once per minute) to help
            // identify the root cause, then clamp to 1 rather than letting Lucene throw an
            // IllegalStateException that corrupts the shard.
            final long now = System.nanoTime();
            if (lastWarnNanos == 0 || now - lastWarnNanos >= WARN_INTERVAL_NANOS) {
                lastWarnNanos = now;
                logger.warn(
                    "Similarity [{}] returned 0 from computeNorm for field [{}] with length {}; "
                        + "clamping to 1 to prevent shard corruption. "
                        + "Check your analysis chain for filters that produce only overlap tokens "
                        + "(positionIncrement == 0).",
                    in,
                    state.getName(),
                    state.getLength()
                );
            }
            return 1L;
        }
        return norm;
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return in.scorer(boost, collectionStats, termStats);
    }

}
