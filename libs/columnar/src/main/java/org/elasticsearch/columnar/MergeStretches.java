/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

/**
 * Where a source segment's documents land next to one another in the segment a merge writes. A stretch is a
 * run of the source's documents that all survive the merge and take consecutive ids in its target, in the
 * order the source holds them, so whatever a column stores for them in that order it can store there too.
 *
 * <p>Without an index sort a merge appends each segment whole, and only a deleted document ends a stretch.
 * Under an index sort documents of different segments may be interleaved, which ends one as well. That test is
 * a conservative one: a document of another segment ends a stretch even when it holds nothing for the field
 * being written.
 *
 * <p>Asked in ascending document order, as a merge cursor steps through the segment. A stretch is found once and
 * answered from for every document inside it, so a field's merge looks at each document once at most.
 */
final class MergeStretches {

    /** Fewest documents a stretch holds to be offered to a writer; see {@link #worthOffering}. */
    static final int MIN_OFFERED_DOCS = 32;

    private final MergeState.DocMap docMap;
    private final Bits liveDocs;
    private final int maxDoc;
    private final boolean interleaved;

    /** The stretch last found, {@code [start, end)}, with {@code end} at {@code maxDoc} for one to the segment's end. */
    private int start = -1;
    private int end = -1;

    MergeStretches(MergeState mergeState, int segment) {
        this(mergeState.docMaps[segment], mergeState.liveDocs[segment], mergeState.maxDocs[segment], mergeState.needsIndexSort);
    }

    MergeStretches(MergeState.DocMap docMap, Bits liveDocs, int maxDoc, boolean interleaved) {
        this.docMap = docMap;
        this.liveDocs = liveDocs;
        this.maxDoc = maxDoc;
        this.interleaved = interleaved;
    }

    /**
     * The first document at or after {@code doc} that does not continue the stretch {@code doc} is in, or
     * {@link DocIdSetIterator#NO_MORE_DOCS} when the stretch runs to the end of the segment. {@code doc} is one the
     * merge keeps.
     */
    int contiguousEnd(int doc) {
        assert doc >= start : "asked for document " + doc + " after " + start;
        if (doc >= end) {
            start = doc;
            end = find(doc);
        }
        return end == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : end;
    }

    /**
     * Whether a stretch from {@code doc} to {@code end}, as {@link #contiguousEnd} answers, is long enough to be worth
     * offering to a writer. What a writer takes from a stretch is chunks, or blocks of ordinals, that lie wholly inside
     * it, and a block holds more values than this; a shorter stretch only costs the writer the attempt, and under an index
     * sort that interleaves segments finely, nearly every stretch is one. A multi-valued column can hold a block in fewer
     * documents, and loses what it could have copied from them.
     */
    static boolean worthOffering(int doc, int end) {
        return end == DocIdSetIterator.NO_MORE_DOCS || end - doc >= MIN_OFFERED_DOCS;
    }

    private int find(int doc) {
        if (interleaved == false && liveDocs == null) {
            // Appended whole and nothing dropped: the rest of the segment lands in order.
            return maxDoc;
        }
        final int mapped = docMap.get(doc);
        assert mapped >= 0 : "document " + doc + " is not kept by the merge";
        if (liveDocs == null) {
            return doc + 1 + consecutiveAfter(doc, mapped);
        }
        int next = doc + 1;
        // A deleted document maps to -1, so the mapping test covers it; live documents are checked as well so
        // the stretch does not rest on that alone.
        while (next < maxDoc && liveDocs.get(next) && docMap.get(next) == mapped + (next - doc)) {
            next++;
        }
        return next;
    }

    /**
     * How many documents after {@code doc} take the ids that follow its own, {@code mapped}, one after another. Nothing is
     * dropped, and a merge hands a segment's documents out in the order it holds them, so their ids only ever grow: the
     * document {@code k} after {@code doc} has id {@code mapped + k} exactly when every one before it does as well, and
     * once one does not none after it does. That is what allows a search for where it stops, which asks about a few
     * documents where stepping through them asks about every one.
     */
    private int consecutiveAfter(int doc, int mapped) {
        final int last = maxDoc - 1 - doc;
        // Doubles the distance until a document breaks the stretch or the segment ends...
        int known = 0;
        int step = 1;
        while (known + step <= last && docMap.get(doc + known + step) == mapped + known + step) {
            known += step;
            step <<= 1;
        }
        // ...then the stretch ends in (known, known + step), or at the segment's end.
        int unknown = (int) Math.min((long) known + step, (long) last + 1);
        while (unknown - known > 1) {
            final int middle = (known + unknown) >>> 1;
            if (docMap.get(doc + middle) == mapped + middle) {
                known = middle;
            } else {
                unknown = middle;
            }
        }
        return known;
    }
}
