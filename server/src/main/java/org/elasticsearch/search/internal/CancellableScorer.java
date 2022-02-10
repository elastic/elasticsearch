/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Objects;

/**
 * A wrapper around scorer that executes checkCancelled on each document access.
 *
 * The main purpose of this class is to allow cancellation of search requests. Note that this class doesn't wrap bulk scorer, for that
 * use {@link CancellableBulkScorer} instead. We have to extend Scorer here instead of extending a much more convenient FilterScorer
 * because we need to override some methods that are marked as final there.
 */
public class CancellableScorer extends Scorer {
    private static final int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    private final Scorer scorer;
    private final Runnable checkCancelled;
    private int seen = 0;

    public CancellableScorer(Scorer scorer, Runnable checkCancelled) {
        super(scorer.getWeight());
        this.scorer = Objects.requireNonNull(scorer);
        this.checkCancelled = Objects.requireNonNull(checkCancelled);
    }

    @Override
    public float score() throws IOException {
        return scorer.score();
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        DocIdSetIterator iterator = scorer.iterator();
        return new DocIdSetIterator() {
            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                    checkCancelled.run();
                }
                return iterator.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                    checkCancelled.run();
                }
                return iterator.advance(target);
            }

            @Override
            public long cost() {
                return iterator.cost();
            }
        };
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        checkCancelled.run();
        return scorer.getMaxScore(upTo);
    }
}
