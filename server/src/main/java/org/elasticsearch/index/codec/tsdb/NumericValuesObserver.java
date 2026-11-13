/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import java.io.IOException;

/**
 * Observes the single pass that {@link TSDBDocValuesBlockWriter} makes over a numeric field's
 * values while it encodes them.
 *
 * <p>Iterating doc values is not free: on flush the values come out of the indexing buffer
 * through the index-sorting wrappers, and on merge out of a merge-sorted view over all source
 * segments. Anything that needs to see every value (currently the doc values skip index) should
 * therefore piggyback on the encode loop through this interface rather than run its own pass.
 *
 * <p>{@link #finish} runs after the last value and <em>before</em> the block writer emits the
 * field's own metadata, so an observer can write metadata that the reader expects to precede the
 * field entry.
 */
public interface NumericValuesObserver {

    /**
     * Called once per document that has at least one value, before any {@link #addValue} call
     * for that document.
     *
     * @param docId      the document
     * @param valueCount number of values in the document, including {@code firstValue}
     * @param firstValue the document's first value
     */
    void startDoc(int docId, int valueCount, long firstValue) throws IOException;

    /**
     * Called for every value of the current document after the first one.
     */
    void addValue(long value);

    /**
     * Called once after the last document, before the field's metadata is written.
     */
    void finish() throws IOException;
}
