/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;

/**
 * Shared encoding logic for the {@code startDoc[]} column that both the Sorted and SortedSet run-table
 * layouts write identically. Concrete subclasses own the full encode/decode logic for their respective types.
 */
abstract class AbstractRunTableLayout {

    static final int BLOCK_SHIFT = 16;

    AbstractRunTableLayout() {}

    /**
     * Writes {@code startDoc[]} via {@link DirectMonotonicWriter} and returns the number of bytes written
     * to {@code data} so the caller can record {@code startDocsLength} in meta.
     */
    protected static long writeStartDocs(final int[] startDocs, int numRuns, final IndexOutput data, final IndexOutput meta)
        throws IOException {
        final long before = data.getFilePointer();
        final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, data, numRuns, BLOCK_SHIFT);
        for (int run = 0; run < numRuns; run++) {
            writer.add(startDocs[run]);
        }
        writer.finish();
        return data.getFilePointer() - before;
    }

    /**
     * Reads {@link DirectMonotonicReader.Meta} for {@code startDoc[]} from the meta stream.
     */
    protected static DirectMonotonicReader.Meta readStartDocsMeta(final IndexInput meta, int numRuns, int blockShift) throws IOException {
        return DirectMonotonicReader.loadMeta(meta, numRuns, blockShift);
    }

    /**
     * Builds the {@link DirectMonotonicReader} for {@code startDoc[]} from a random-access slice of {@code data}.
     */
    protected static DirectMonotonicReader openStartDocs(
        final DirectMonotonicReader.Meta startDocsMeta,
        final IndexInput data,
        long dataStart,
        long startDocsLength
    ) throws IOException {
        return DirectMonotonicReader.getInstance(startDocsMeta, data.randomAccessSlice(dataStart, startDocsLength));
    }
}
