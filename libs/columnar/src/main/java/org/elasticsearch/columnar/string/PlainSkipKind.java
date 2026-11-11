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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;

/**
 * A way of writing and reading the skip region of a plain column. The column's metadata records the {@link #id()} of
 * the kind that wrote the region, and a reader finds the kind from it, so a region is always read by the kind that wrote
 * it and a new kind never changes how an existing one is read. An id is never reused once shipped.
 *
 * <p>A region is a list of entries. Each entry owns a contiguous range of the column's slots, and the entries together
 * cover all of them. A reader answers which entries a query rules out; the caller still compares the values of the
 * slots in the entries that are kept.
 */
interface PlainSkipKind {

    /** The id recorded in column metadata. Zero is reserved for a column that has no region. */
    byte id();

    Writer newWriter(Directory directory, IOContext context, String dataName, long numValues) throws IOException;

    Reader newReader(StringColumnMetadata.Plain.Skipper skipper, long numValues, IndexInput skipIndex) throws IOException;

    /** The kind that wrote a region with this id. */
    static PlainSkipKind forId(byte id) {
        return switch (id) {
            case SortedSeparatorsSkip.ID -> SortedSeparatorsSkip.INSTANCE;
            default -> throw new IllegalArgumentException("unknown plain skip kind [" + id + "]");
        };
    }

    /** The kind a column that asks for a skip region is written with. */
    static PlainSkipKind forWriting() {
        return SortedSeparatorsSkip.INSTANCE;
    }

    /** Takes the slots of a column in order, then appends the region they describe to the skip-index file. */
    interface Writer extends Closeable {

        /**
         * Takes the slot at {@code address}, held by {@code doc} and owned by {@code chunk}; a null {@code value} is a
         * null slot.
         */
        void add(long address, int doc, int chunk, BytesRef value) throws IOException;

        /** Appends the region to {@code skipIndex} and returns where it went, or null if there is no region. */
        StringColumnMetadata.Plain.Skipper finish(IndexOutput skipIndex) throws IOException;
    }

    /** Reads a region. An instance follows one query and is not shared. */
    interface Reader {

        int entryCount();

        int firstDoc(int entry) throws IOException;

        long firstSlot(int entry) throws IOException;

        /** The slot after the last one {@code entry} owns. */
        long endSlot(int entry) throws IOException;

        /** The entry that owns {@code slot}. */
        int entryOf(long slot) throws IOException;

        /** Whether the region proves that no slot {@code entry} owns can match {@code query}. */
        boolean excludes(int entry, PlainSkipQuery query) throws IOException;

        /** The first entry at or after {@code fromEntry} that {@code query} does not rule out, or {@link #entryCount()}. */
        int nextCandidate(int fromEntry, PlainSkipQuery query) throws IOException;
    }
}
