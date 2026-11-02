/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * A forward, streaming cursor over a numeric column's values on the write path — the input the encoder
 * pipeline pulls from. It iterates the documents that have a value ({@link DocIdSetIterator}) and, for
 * the current document, yields its values in written order. Nothing is materialized: on ingest it
 * decodes one payload at a time, and on merge it reads one block at a time off the mapped data input.
 */
public abstract class NumericColumnValues extends DocIdSetIterator {

    /** The number of values the current document holds. */
    public abstract int valueCount();

    /** The next value of the current document; call exactly {@link #valueCount()} times per document. */
    public abstract long nextValue() throws IOException;

    /**
     * Reads the values of the documents after the current one into {@code dst}, as many as fit or are left, and
     * answers how many, leaving the cursor on the last of them; zero once none are left. Only a cursor whose every
     * document holds one value, with ids following one another, can: any other answers -1 without moving, and is read a
     * document at a time. A column of that shape has nothing to learn from each document but its value, so a cursor
     * that knows it is spared being stepped through value by value.
     */
    public int nextValuesOfConsecutiveDocuments(long[] dst) {
        return -1;
    }
}
