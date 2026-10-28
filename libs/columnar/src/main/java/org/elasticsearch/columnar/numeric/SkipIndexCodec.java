/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * The pluggable skip index of a numeric column, built inline during the value-encode pass. The concrete
 * codec is chosen per column and recorded by {@link #id()} on the column's {@code Skipper} metadata; a
 * reader dispatches on it via {@link #forId(byte)}. Ids are frozen once shipped.
 */
public interface SkipIndexCodec {

    /** Default multi-level skip codec id. */
    byte MULTI_LEVEL_ID = 0;

    /** Frozen identifier persisted in column metadata. Never reuse or repurpose an id. */
    byte id();

    /** A fresh writer fed the column's values in doc order during the value-encode pass. */
    Writer writer();

    /** A reader over the skip region this codec wrote, positioned by {@code meta} within {@code data}. */
    DocValuesSkipper reader(NumericColumnMetadata.Skipper meta, IndexInput data) throws IOException;

    /** Resolves a codec from its persisted id. */
    static SkipIndexCodec forId(byte id) {
        if (id == MULTI_LEVEL_ID) {
            return new MultiLevelSkipIndexCodec();
        }
        throw new IllegalArgumentException("Unknown skip-index codec id: " + id);
    }

    /**
     * Fed per document during the value-encode pass and flushed after the values. Encoded skip bytes are
     * buffered internally and appended to the data output only in {@link #finish}, since the value blocks
     * are written to the same output while this writer is being fed.
     */
    interface Writer {
        /** Begin a document with {@code valueCount} values, to be delivered by {@link #add}. */
        void startDoc(int doc, int valueCount);

        /** One value of the current document, in written order. */
        void add(long value);

        /** Appends the buffered skip region to {@code data} and returns its metadata. */
        NumericColumnMetadata.Skipper finish(IndexOutput data) throws IOException;
    }
}
