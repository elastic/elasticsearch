/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Helper iterator to visit a predefined fixed number of documents per ordinal. It is assumed that
 * the {@link SortedDocValues} are in order and the iterator will be used as a competitive iterator.
 * Because docs might not be used due to query constraints, we need to acknowledge that a document
 * was actually consumed by calling {@link #visitedDoc(int)}.
 */
public class DocsPerOrdIterator extends DocIdSetIterator {
    private final SortedDocValues tsids;
    private final int docsPerOrd;
    private int currentOrd;
    private int currentDocsPerOrd;

    public DocsPerOrdIterator(SortedDocValues tsids, int docsPerOrd) {
        this.tsids = tsids;
        this.docsPerOrd = docsPerOrd;
    }

    /**
     * Acknowledge that the document is actually consumed.
     */
    public void visitedDoc(int doc) throws IOException {
        if (doc != tsids.docID()) {
            // only update counters if the visited doc comes from this iterator
            return;
        }
        final int ord = tsids.ordValue();
        if (ord == currentOrd) {
            currentDocsPerOrd++;
        } else {
            currentOrd = ord;
            currentDocsPerOrd = 1;
        }
    }

    @Override
    public int docID() {
        return tsids.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        if (currentDocsPerOrd == docsPerOrd) {
            currentDocsPerOrd = 0;
            return advanceTSID();
        }
        return tsids.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        if (currentDocsPerOrd == docsPerOrd) {
            currentDocsPerOrd = 0;
            advanceTSID();
        }
        if (tsids.docID() < target) {
            return tsids.advance(target);
        }
        return tsids.docID();
    }

    @Override
    public long cost() {
        return tsids.cost();
    }

    private int advanceTSID() throws IOException {
        int docID = tsids.docID();
        if (docID == DocIdSetIterator.NO_MORE_DOCS) {
            return docID;
        }
        int currentOrd = tsids.ordValue();
        do {
            // if we could know efficiently the docId for the next tsid, we could
            // speed up this process enormously instead of manually advance the iterator
            // until we find the position we want.
            docID = tsids.nextDoc();
        } while (docID != DocIdSetIterator.NO_MORE_DOCS && currentOrd == tsids.ordValue());
        return docID;
    }
}
