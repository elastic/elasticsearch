/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * The kind for a plain column whose values are in order, which is the case for the primary field of the index sort.
 * Each entry, a compressed chunk, records the first value at or after its first slot, its separator. Because the
 * values are in order, every value an entry owns lies between its separator and the separator of the next entry, so
 * those two bound it and a reader finds the entries a query can touch by searching the separators, without
 * decompressing any chunk. The writer checks the order as it goes and writes no region when the values are not in
 * order, as for a field with several values per document.
 *
 * <p>The region is a header (a byte that says whether the values descend, and the payload offset of the last value),
 * a directory of fixed size entries (the first slot an entry owns, the document holding it and the payload offset of
 * its separator), and the payload: for each separator its length and bytes. An entry that owns no value takes the
 * separator of the next one that does, and an entry after the last value takes the last value.
 */
final class SortedSeparatorsSkip implements PlainSkipKind {

    static final byte ID = 1;

    static final SortedSeparatorsSkip INSTANCE = new SortedSeparatorsSkip();

    static final byte ASCENDING = 0;

    static final byte DESCENDING = 1;

    static final int HEADER_BYTES = Byte.BYTES + Long.BYTES;

    static final int ENTRY_BYTES = Long.BYTES + Integer.BYTES + Long.BYTES;

    static final int DOC_FIELD = Long.BYTES;

    static final int OFFSET_FIELD = Long.BYTES + Integer.BYTES;

    static final int LENGTH_BYTES = Integer.BYTES;

    private SortedSeparatorsSkip() {}

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public Writer newWriter(Directory directory, IOContext context, String dataName, long numValues) throws IOException {
        return new SortedSeparatorsWriter(directory, context, dataName, numValues);
    }

    @Override
    public Reader newReader(StringColumnMetadata.Plain.Skipper skipper, long numValues, IndexInput skipIndex) throws IOException {
        return new SortedSeparatorsReader(skipper, numValues, skipIndex);
    }
}
