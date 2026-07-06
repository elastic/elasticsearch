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
 * <p>Ordinal fields use the format-level block size and carry no per-field block metadata. An
 * in-development version ({@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig#VERSION_ORDINAL_BLOCK_SHIFT})
 * once wrote a per-field {@code blockShift} byte, removed at
 * {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig#VERSION_REMOVE_ORDINAL_BLOCK_SHIFT}.
 * Because the ES95 codec was never released, no segment carrying that byte exists, so the read
 * path is dropped rather than kept for backward compatibility.
 */
final class ES95OrdinalFieldReader implements OrdinalFieldReader {

    private static final TSDBDocValuesBlockReader BLOCK_READER = new TSDBDocValuesBlockReader();

    @Override
    public void readFieldEntry(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
        BLOCK_READER.readFieldEntry(meta, entry, numericBlockShift);
    }

    @Override
    public Decoder decoder(final int blockSize) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(blockSize);
        return encoder::decodeOrdinals;
    }
}
