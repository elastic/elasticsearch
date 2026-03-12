/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Objects;

public final class XUpdateableDocIdSetIterator extends AbstractDocIdSetIterator {

    private DocIdSetIterator in = DocIdSetIterator.empty();

    /**
     * Update the wrapped {@link DocIdSetIterator}. It doesn't need to be positioned on the same doc
     * ID as this iterator.
     */
    public void update(DocIdSetIterator iterator) {
        this.in = Objects.requireNonNull(iterator);
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        int curDoc = in.docID();
        if (curDoc < target) {
            curDoc = in.advance(target);
        }
        return this.doc = curDoc;
    }

    @Override
    public long cost() {
        return in.cost();
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        // #update may have been just called
        if (in.docID() < doc) {
            in.advance(doc);
        }
        in.intoBitSet(upTo, bitSet, offset);
        doc = in.docID();
    }

    @Override
    public int docIDRunEnd() throws IOException {
        // #update may have been just called
        if (in.docID() < doc) {
            in.advance(doc);
        }
        if (in.docID() == doc) {
            return in.docIDRunEnd();
        } else {
            return super.docIDRunEnd();
        }
    }

    public DocIdSetIterator getDelegate() {
        return in;
    }
}
