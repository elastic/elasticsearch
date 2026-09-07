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
 * Where a page of a string column is handed to whoever asked for it.
 *
 * <p>A page carries the values of a run of documents, and a document may hold several of them or none. So what arrives
 * is a flat run of values and, beside it, how many of them each document took: {@code valueCounts} holds one entry per
 * document, and is null where every document took exactly one, which is the common shape and the one worth not
 * allocating for. A document that took none is a document with no value to offer - its slots were all null - and the
 * count says so with a zero.
 *
 * <p>Null slots never arrive. A null is not a value a consumer of a page can hold, so a document's nulls are dropped
 * and its count is of what remains; a document of two slots with one null arrives holding one value.
 */
public interface StringBlockSink {

    /**
     * A page as ordinals into its own distinct values, for a page that repeats enough to be worth naming them. The
     * ordinals index {@code dictionary}, which holds {@code dictionarySize} entries and is valid until the next call.
     *
     * @param ordinals    one entry per value, {@code valueCount} of them
     * @param valueCount  values across every document in the page
     * @param valueCounts values per document, or null when every document holds exactly one
     * @param docCount    documents the page covers
     */
    void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int dictionarySize);

    /** A page as its values, for a page that repeats too little for ordinals to pay. Shaped as above. */
    void appendValues(BytesRef[] values, int valueCount, int[] valueCounts, int docCount);
}
