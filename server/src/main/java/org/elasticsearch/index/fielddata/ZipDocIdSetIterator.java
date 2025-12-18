/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Advances two DocIdSetIterators together. Require that they have the same set of docs.
 */
public class ZipDocIdSetIterator extends DocIdSetIterator {

    private final DocIdSetIterator a;
    private final DocIdSetIterator b;

    public ZipDocIdSetIterator(DocIdSetIterator a, DocIdSetIterator b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public int docID() {
        return a.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        int resA = a.nextDoc();
        int resB = b.nextDoc();
        assert resA == resB : "Doc IDs do not match";
        return resA;
    }

    @Override
    public int advance(int target) throws IOException {
        int resA = a.advance(target);
        int resB = b.advance(target);
        assert resA == resB : "Doc IDs do not match";
        return resA;
    }

    @Override
    public long cost() {
        return 0;
    }
}
