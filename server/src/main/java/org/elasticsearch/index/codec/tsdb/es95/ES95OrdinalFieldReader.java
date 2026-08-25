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
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * {@link OrdinalFieldReader} for the ES95 TSDB format. Segments written in the range
 * [{@link TSDBDocValuesFormatConfig#VERSION_ORDINAL_BLOCK_SHIFT},
 * {@link TSDBDocValuesFormatConfig#VERSION_REMOVE_ORDINAL_BLOCK_SHIFT}) carry a per-field
 * {@code blockShift} byte that {@link #readFieldEntry} reads; later segments carry none. The write
 * path no longer emits the byte; this read path stays for upgrades from earlier binaries.
 */
final class ES95OrdinalFieldReader implements OrdinalFieldReader {

    private static final TSDBDocValuesBlockReader BLOCK_READER = new TSDBDocValuesBlockReader();

    private final int segmentVersion;

    ES95OrdinalFieldReader(final int segmentVersion) {
        this.segmentVersion = segmentVersion;
    }

    @Override
    public void readFieldEntry(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
        if (segmentVersion >= TSDBDocValuesFormatConfig.VERSION_ORDINAL_BLOCK_SHIFT
            && segmentVersion < TSDBDocValuesFormatConfig.VERSION_REMOVE_ORDINAL_BLOCK_SHIFT) {
            BLOCK_READER.readFieldEntry(meta, entry, numericBlockShift, m -> {
                final int blockShift = m.readByte() & 0xFF;
                entry.blockSize = 1 << blockShift;
            });
        } else {
            BLOCK_READER.readFieldEntry(meta, entry, numericBlockShift);
        }
    }

    @Override
    public Decoder decoder(final int blockSize) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(blockSize);
        return encoder::decodeOrdinals;
    }
}
