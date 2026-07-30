/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Writes a field's column-iterator structure to the data file and returns its {@link ColumnIteratorMetadata}.
 * The empty and fully-dense fields write nothing — their shape is captured entirely in metadata.
 */
public final class ColumnIteratorWriter {

    private ColumnIteratorWriter() {}

    /**
     * @param docsWithField   documents that have a value, in increasing id order; consumed once,
     *                        and only for a sparse field
     * @param numDocsWithField number of documents that have a value (the cardinality)
     * @param maxDoc          number of documents in the segment
     * @param data            output the sparse structure is appended to
     */
    public static ColumnIteratorMetadata write(DocIdSetIterator docsWithField, int numDocsWithField, int maxDoc, IndexOutput data)
        throws IOException {
        if (numDocsWithField == 0) {
            return ColumnIteratorMetadata.empty(maxDoc);
        }
        if (numDocsWithField == maxDoc) {
            return ColumnIteratorMetadata.dense(maxDoc);
        }
        long offset = data.getFilePointer();
        short jumpTableEntryCount = IndexedDISI.writeBitSet(docsWithField, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
        long length = data.getFilePointer() - offset;
        return new ColumnIteratorMetadata(
            offset,
            length,
            jumpTableEntryCount,
            IndexedDISI.DEFAULT_DENSE_RANK_POWER,
            numDocsWithField,
            maxDoc
        );
    }
}
