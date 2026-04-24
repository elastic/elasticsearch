/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * {@link OrdinalFieldWriter} implementation that writes the ordinal stream of sorted and
 * sorted-set fields using the TSDB block layout.
 *
 * <p>{@link #writeFieldEntry} computes the per-ordinal bit width, then delegates to the shared
 * write loop in {@link TSDBDocValuesBlockWriter} with the {@link Encoder} supplied at
 * construction time.
 */
public final class TSDBOrdinalFieldWriter implements OrdinalFieldWriter {

    private final NumericWriteContext ctx;
    private final Encoder encoder;

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    /**
     * @param ctx     segment-scoped write state shared by every field in this segment
     * @param encoder per-block encoder that supplies the codec-specific ordinal encoding
     */
    public TSDBOrdinalFieldWriter(final NumericWriteContext ctx, final Encoder encoder) {
        this.ctx = ctx;
        this.encoder = encoder;
    }

    @Override
    public Encoder encoder() {
        return encoder;
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        long maxOrd,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        final int bitsPerOrd = PackedInts.bitsRequired(Math.max(maxOrd - 1, 0));
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            maxOrd,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> encoder().encodeOrdinals(buffer, data, bitsPerOrd)
        );
    }
}
