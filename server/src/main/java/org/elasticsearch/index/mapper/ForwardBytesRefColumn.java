/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnData;

/**
 * Reads one scalar var-width ESCF column document by document, in increasing document order, without allocating per document: the
 * value of the current document is the cursor's reusable {@link BytesRef}, valid until the next {@link #advance}. This is how a
 * {@link SourceColumn.Layer} walks the column its mapper produced, in step with the blob writer that visits documents in order.
 */
final class ForwardBytesRefColumn {

    private final ObjectTupleCursor<BytesRef> cursor;
    /** The document the cursor is on, or {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS} after the last one. */
    private int cursorDoc;

    /** @param data a scalar (non-array) column of byte strings */
    ForwardBytesRefColumn(EscfColumnData data) {
        this.cursor = EscfColumn.from(data).bytesRefCursor(false);
        this.cursorDoc = cursor.nextDoc();
    }

    /**
     * Moves to {@code doc}, which must not be smaller than the document of the previous call.
     *
     * @return whether the column holds a value for {@code doc}
     */
    boolean advance(int doc) {
        while (cursorDoc < doc) {
            cursorDoc = cursor.nextDoc();
        }
        return cursorDoc == doc;
    }

    /** The current document's value; only meaningful when the last {@link #advance} returned {@code true}. */
    BytesRef value() {
        return cursor.value();
    }
}
