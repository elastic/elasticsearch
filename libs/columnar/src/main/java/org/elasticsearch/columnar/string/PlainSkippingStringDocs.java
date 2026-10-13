/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * The documents of a plain column that can hold a value in a range, found without visiting the documents of
 * an interval whose term bounds miss it. Wraps the documents that hold a value and, where a document falls in
 * a span the bounds rule out, advances them past the whole span. Of the levels covering a document it takes the
 * coarsest that misses, so a group of intervals that misses the range costs one entry read.
 *
 * <p>The documents it keeps are a superset of those holding a value in the range, so a confirmation still has
 * to compare the values unless {@link #intervalInsideRange} says the document's interval cannot fail it.
 */
final class PlainSkippingStringDocs extends DocIdSetIterator {

    private final DocIdSetIterator presence;
    private final PlainStringColumnSkipper skipper;
    private final BytesRef lower;
    private final boolean includeLower;
    private final BytesRef upper;
    private final boolean includeUpper;

    private int intervalLastDoc = -1;
    private int coarsestMissingLevel = -1;
    private boolean intervalInsideRange;

    /** A null bound is open. */
    PlainSkippingStringDocs(
        DocIdSetIterator presence,
        PlainStringColumnSkipper skipper,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper
    ) {
        this.presence = presence;
        this.skipper = skipper;
        this.lower = lower;
        this.includeLower = includeLower;
        this.upper = upper;
        this.includeUpper = includeUpper;
    }

    @Override
    public int docID() {
        return presence.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        final int doc = presence.nextDoc();
        return doc <= intervalLastDoc ? doc : skipMissingSpans(doc);
    }

    @Override
    public int advance(int target) throws IOException {
        final int doc = presence.advance(target);
        return doc <= intervalLastDoc ? doc : skipMissingSpans(doc);
    }

    @Override
    public long cost() {
        return presence.cost();
    }

    /**
     * Whether every non-null value of the current document's interval is in the range, so a document of it
     * that holds a slot matches without its value being read. Only a column with no null slots can say so,
     * since a document holding nothing but nulls is in the interval and matches nothing. A document holding no
     * slot at all, which an empty array is, is in the interval too, so the caller has to count its slots.
     */
    boolean intervalInsideRange() {
        return intervalInsideRange;
    }

    private int skipMissingSpans(int doc) throws IOException {
        while (doc != NO_MORE_DOCS) {
            if (intervalLastDoc < doc) {
                skipper.advance(doc);
                intervalLastDoc = skipper.maxDocID(0);
                if (intervalLastDoc == NO_MORE_DOCS) {
                    return doc;
                }
                classifyInterval();
            }
            if (coarsestMissingLevel < 0) {
                return doc;
            }
            doc = presence.advance(skipper.maxDocID(coarsestMissingLevel) + 1);
        }
        return NO_MORE_DOCS;
    }

    private void classifyInterval() {
        coarsestMissingLevel = -1;
        for (int level = skipper.numLevels() - 1; level >= 0 && coarsestMissingLevel < 0; level--) {
            if (boundsMissRange(skipper.minTerm(level), skipper.maxTerm(level))) {
                coarsestMissingLevel = level;
            }
        }
        intervalInsideRange = coarsestMissingLevel < 0 && boundsInsideRange(skipper.minTerm(0), skipper.maxTerm(0));
    }

    private boolean boundsInsideRange(BytesRef min, BytesRef max) {
        final int lowerCmp = lower == null ? 1 : min.compareTo(lower);
        final int upperCmp = upper == null ? -1 : max.compareTo(upper);
        return (lowerCmp > 0 || (lowerCmp == 0 && includeLower)) && (upperCmp < 0 || (upperCmp == 0 && includeUpper));
    }

    private boolean boundsMissRange(BytesRef min, BytesRef max) {
        if (upper != null) {
            final int cmp = min.compareTo(upper);
            if (cmp > 0 || (cmp == 0 && includeUpper == false)) {
                return true;
            }
        }
        if (lower != null) {
            final int cmp = max.compareTo(lower);
            return cmp < 0 || (cmp == 0 && includeLower == false);
        }
        return false;
    }
}
