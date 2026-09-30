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

    private final MergeState.DocMap docMap;
    private final Bits liveDocs;
    private final int maxDoc;
    private final boolean interleaved;

    /** The stretch last found, {@code [start, end)}, with {@code end} at {@code maxDoc} for one to the segment's end. */
    private int start = -1;
    private int end = -1;

    MergeStretches(MergeState mergeState, int segment) {
        this.docMap = mergeState.docMaps[segment];
        this.liveDocs = mergeState.liveDocs[segment];
        this.maxDoc = mergeState.maxDocs[segment];
        this.interleaved = mergeState.needsIndexSort;
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

    private int find(int doc) {
        if (interleaved == false && liveDocs == null) {
            // Appended whole and nothing dropped: the rest of the segment lands in order.
            return maxDoc;
        }
        final int mapped = docMap.get(doc);
        assert mapped >= 0 : "document " + doc + " is not kept by the merge";
        int next = doc + 1;
        // A deleted document maps to -1, so the mapping test covers it; live documents are checked as well so
        // the stretch does not rest on that alone.
        while (next < maxDoc && (liveDocs == null || liveDocs.get(next)) && docMap.get(next) == mapped + (next - doc)) {
            next++;
        }
        return next;
    }
}
