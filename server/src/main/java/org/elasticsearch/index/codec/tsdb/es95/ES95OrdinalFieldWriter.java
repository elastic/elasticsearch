/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockWriter;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;

import java.io.IOException;

/**
 * {@link OrdinalFieldWriter} for the ES95 TSDB format. Ordinals are encoded at the format-level
 * block size and carry no per-field block metadata. Earlier binaries wrote a per-field
 * {@code blockShift} byte here, removed at
 * {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig#VERSION_REMOVE_ORDINAL_BLOCK_SHIFT};
 * see {@link ES95OrdinalFieldReader} for the read side that still handles it.
 */
final class ES95OrdinalFieldWriter implements OrdinalFieldWriter {

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    private final NumericWriteContext ctx;

    ES95OrdinalFieldWriter(final NumericWriteContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public Encoder encoder() {
        final TSDBDocValuesEncoder enc = new TSDBDocValuesEncoder(ctx.blockSize());
        return enc::encodeOrdinals;
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        long maxOrd,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        final int blockSize = ctx.blockSize();
        final int bitsPerOrd = PackedInts.bitsRequired(Math.max(maxOrd - 1, 0));
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(blockSize);
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            maxOrd,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> encoder.encodeOrdinals(buffer, data, bitsPerOrd),
            blockSize
        );
    }
}
