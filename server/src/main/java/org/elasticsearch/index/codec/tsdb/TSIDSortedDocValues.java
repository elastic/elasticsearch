/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class TSIDSortedDocValues extends SortedDocValues {


    public static TSIDSortedDocValues toTSIDSortedDocValues(SortedDocValues sortedDocValues) {
        if (sortedDocValues instanceof TSIDSortedDocValues tsidSortedDocValues) {
            return tsidSortedDocValues;
        }
        return new TSIDSortedDocValues(sortedDocValues);
    }

    private final SortedDocValues docValues;

    public TSIDSortedDocValues(SortedDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public int ordValue() throws IOException {
        return docValues.ordValue();
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
        return docValues.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
        return docValues.getValueCount();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return docValues.advanceExact(target);
    }

    @Override
    public int docID() {
        return docValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return docValues.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return docValues.advance(target);
    }

    @Override
    public long cost() {
        return docValues.cost();
    }

    /**
     * Advances to the first beyond the current whose ordinal is distinct to the current ordinal and
     * returns the document number itself. Exhausts the iterator and returns {@link #NO_MORE_DOCS} if
     * theerre is no more ordinals distinct to the current one.
     *
     * <p>The behavior of this method is <b>undefined</b> when called with <code> target &le; current
     * </code>, or after the iterator has exhausted. Both cases may result in unpredicted behavior.
     *
     * <p>When <code> target &gt; current</code> it behaves as if written:
     *
     * <pre class="prettyprint">
     * int advanceOrd() {
     *   final long ord = ordValue();
     *   int doc = docID();
     *   while (doc != DocIdSetIterator.NO_MORE_DOCS &amp;&amp; ordValue() == ord) {
     *       doc = nextDoc();
     *    }
     *   return doc;
     * }
     * </pre>
     *
     * Some implementations are considerably more efficient than that.
     */
    public int advanceOrd() throws IOException {
        int doc = docID();
        if (doc == DocIdSetIterator.NO_MORE_DOCS || doc == -1) {
            return doc;
        }
        final long ord = ordValue();
        do {
            doc = nextDoc();
        } while (doc != DocIdSetIterator.NO_MORE_DOCS && ordValue() == ord);
        assert doc == docID();
        return doc;
    }
}
