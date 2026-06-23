/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;

import java.io.IOException;

/**
 * {@link OrdinalFieldReader} implementation for the ES95 TSDB format.
 *
 * <p>{@link #readFieldEntry} delegates to {@link TSDBDocValuesBlockReader} with a
 * per-field metadata callback that reads the {@code blockShift} byte written by
 * {@link ES95OrdinalFieldWriter} and sets {@link NumericEntry#blockSize} from it.
 * {@link #decoder(int)} creates a {@link TSDBDocValuesEncoder} sized to that block size,
 * so the decoder is always correctly sized for the field it was encoded with.
 */
final class ES95OrdinalFieldReader implements OrdinalFieldReader {

    private static final TSDBDocValuesBlockReader BLOCK_READER = new TSDBDocValuesBlockReader();

    @Override
    public void readFieldEntry(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
        BLOCK_READER.readFieldEntry(meta, entry, numericBlockShift, m -> {
            final int blockShift = m.readByte() & 0xFF;
            entry.blockSize = 1 << blockShift;
        });
    }

    @Override
    public Decoder decoder(final int blockSize) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(blockSize);
        return encoder::decodeOrdinals;
    }
}
