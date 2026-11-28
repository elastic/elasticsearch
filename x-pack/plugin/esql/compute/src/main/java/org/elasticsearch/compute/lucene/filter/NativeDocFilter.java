/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A Lucene query used as a leaf of the tree. The matching docs of a window are loaded into a bit set with
 * {@link DocIdSetIterator#intoBitSet} and intersected with the docs in play, so nothing advances one doc at a time. A
 * query with a two-phase check cannot be loaded in bulk, because its approximation says only that a doc may match. For
 * that query the check runs on the docs still in play and on no others.
 * <p>
 * The scorer is kept for each segment and built again only when a window starts behind it. Deleted docs are not applied
 * here; {@link DocFilterPass} keeps them out of play.
 */
public final class NativeDocFilter implements DocFilter {
    private final Weight weight;
    private final Map<Integer, SegmentState> segments = new HashMap<>();
    private FixedBitSet scratch;

    public static NativeDocFilter of(IndexSearcher searcher, Query query) throws IOException {
        return new NativeDocFilter(searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f));
    }

    private NativeDocFilter(Weight weight) {
        this.weight = weight;
    }

    @Override
    public void filter(DocWindow window, FixedBitSet inPlay) throws IOException {
        SegmentState state = state(window);
        if (state == null) {
            inPlay.clear();
            return;
        }
        state.nextFrom = window.to();
        if (state.twoPhase != null) {
            confirm(window, state, inPlay);
            return;
        }
        DocIdSetIterator iterator = state.iterator;
        if (iterator.docID() < window.from()) {
            iterator.advance(window.from());
        }
        if (scratch == null || scratch.length() != inPlay.length()) {
            scratch = new FixedBitSet(inPlay.length());
        } else {
            scratch.clear();
        }
        if (iterator.docID() < window.to()) {
            iterator.intoBitSet(window.to(), scratch, window.from());
        }
        inPlay.and(scratch);
    }

    private static void confirm(DocWindow window, SegmentState state, FixedBitSet inPlay) throws IOException {
        DocIdSetIterator approximation = state.iterator;
        for (int bit = inPlay.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bit + 1 < inPlay.length()
            ? inPlay.nextSetBit(bit + 1)
            : DocIdSetIterator.NO_MORE_DOCS) {
            int doc = window.from() + bit;
            if (approximation.docID() < doc) {
                approximation.advance(doc);
            }
            if (approximation.docID() != doc || state.twoPhase.matches() == false) {
                inPlay.clear(bit);
            }
        }
    }

    private SegmentState state(DocWindow window) throws IOException {
        LeafReaderContext leaf = window.leaf();
        SegmentState state = segments.get(leaf.ord);
        if (state != null && window.from() >= state.nextFrom) {
            return state.exhausted ? null : state;
        }
        ScorerSupplier supplier = weight.scorerSupplier(leaf);
        if (supplier == null) {
            segments.put(leaf.ord, SegmentState.EMPTY);
            return null;
        }
        Scorer scorer = supplier.get(Long.MAX_VALUE);
        TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
        state = new SegmentState(twoPhase == null ? scorer.iterator() : twoPhase.approximation(), twoPhase);
        segments.put(leaf.ord, state);
        return state;
    }

    private static final class SegmentState {
        private static final SegmentState EMPTY = new SegmentState(null, null);

        private final DocIdSetIterator iterator;
        private final TwoPhaseIterator twoPhase;
        private final boolean exhausted;
        private int nextFrom;

        private SegmentState(DocIdSetIterator iterator, TwoPhaseIterator twoPhase) {
            this.iterator = iterator;
            this.twoPhase = twoPhase;
            this.exhausted = iterator == null;
        }
    }
}
