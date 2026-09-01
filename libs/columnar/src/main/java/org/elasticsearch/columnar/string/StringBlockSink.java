/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

/**
 * Where {@link StringColumnReader#readBlock} puts a page of values.
 *
 * <p>Two shapes, because a page of a repetitive column is worth handing over as ordinals: a consumer
 * grouping by value then compares an int per document and resolves each distinct value once, rather than
 * hashing bytes once per document. A page with little repetition is handed over as values, since ordinals
 * into a dictionary as long as the page save nothing.
 */
public interface StringBlockSink {

    /**
     * A page as one ordinal per document into {@code dictionary}, which holds the page's distinct values
     * and is valid until the next call. Ordinals index it directly, so they run from zero however the
     * column numbers its terms.
     *
     * <p>Distinct is by the bytes and holds whatever shape the column has: equal values are one entry
     * however far apart the documents carrying them sit, whether the column is in term order, clustered
     * into runs that restart, or in no order at all, and whether or not its vocabulary names them. So the
     * ordinal is an identity within the page, and a consumer grouping by value can group on it and resolve
     * the bytes once an entry rather than once a document.
     */
    void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize);

    /** A page as one value per document, valid until the next call. */
    void appendValues(BytesRef[] values, int count);
}
