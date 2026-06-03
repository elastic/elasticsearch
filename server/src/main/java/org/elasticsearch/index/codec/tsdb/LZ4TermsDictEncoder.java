/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

/**
 * {@link TermsDictFieldWriter.Encoder} that LZ4-compresses each block body against the block's
 * first term (the dictionary prefix). Stateless and shared via {@link #INSTANCE}; the LZ4 hash
 * table lives in a {@link ThreadLocal} so concurrent flushes from different
 * {@code DocumentsWriterPerThread} instances each get their own reusable instance.
 */
public final class LZ4TermsDictEncoder implements TermsDictFieldWriter.Encoder {

    public static final LZ4TermsDictEncoder INSTANCE = new LZ4TermsDictEncoder();

    private final ThreadLocal<LZ4.FastCompressionHashTable> hashTable = ThreadLocal.withInitial(LZ4.FastCompressionHashTable::new);

    private LZ4TermsDictEncoder() {}

    @Override
    public void writeHeader(final IndexOutput data, final int blockLength) throws IOException {
        data.writeVInt(blockLength);
    }

    @Override
    public void writeBody(final IndexOutput data, final byte[] buffer, final int dictOffset, final int dictLength, final int blockLength)
        throws IOException {
        final LZ4.FastCompressionHashTable ht = hashTable.get();
        LZ4.compressWithDictionary(buffer, dictOffset, dictLength, blockLength, data, ht);
    }
}
