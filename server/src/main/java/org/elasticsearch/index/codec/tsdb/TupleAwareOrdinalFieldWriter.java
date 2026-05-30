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
 * {@link OrdinalFieldWriter} implementation that drives the tuple-aware write loop for
 * SORTED_SET fields. The per-block encoder receives the per-doc value counts and the
 * head/tail straddle offsets so it can pick encodings that exploit doc boundaries
 * (e.g. tuple-run encoding for the K-cycle pattern produced by multi-valued docs
 * sharing the same ord set within a {@code _tsid} run).
 *
 * <p>Mirrors {@link TSDBOrdinalFieldWriter} but routes through
 * {@link TSDBDocValuesBlockWriter#writeFieldEntryWithTupleAwareness} instead of the
 * simple block-encoder path.
 */
public final class TupleAwareOrdinalFieldWriter implements OrdinalFieldWriter {

    private final NumericWriteContext ctx;
    private final TupleAwareEncoder encoder;

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    /**
     * @param ctx     segment-scoped write state shared by every field in this segment
     * @param encoder per-block tuple-aware encoder that supplies the codec-specific ordinal encoding
     */
    public TupleAwareOrdinalFieldWriter(final NumericWriteContext ctx, final TupleAwareEncoder encoder) {
        this.ctx = ctx;
        this.encoder = encoder;
    }

    @Override
    public Encoder encoder() {
        throw new UnsupportedOperationException("TupleAwareOrdinalFieldWriter exposes a TupleAwareEncoder, not Encoder");
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
        return BLOCK_WRITER.writeFieldEntryWithTupleAwareness(
            ctx,
            field,
            valuesSource,
            maxOrd,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, perDocK, numDocs, headOffset, tailMissing, data) -> encoder.encodeOrdinals(
                buffer,
                perDocK,
                numDocs,
                headOffset,
                tailMissing,
                data,
                bitsPerOrd
            ),
            null
        );
    }
}
