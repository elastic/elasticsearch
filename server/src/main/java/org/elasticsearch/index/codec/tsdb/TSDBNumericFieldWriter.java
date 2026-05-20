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

import java.io.IOException;

/**
 * {@link NumericFieldWriter} implementation that writes numeric and sorted-numeric fields
 * using the TSDB block layout.
 *
 * <p>{@link #writeFieldEntry} delegates to the shared write loop in {@link TSDBDocValuesBlockWriter}
 * with the {@link Encoder} supplied at construction time.
 */
public final class TSDBNumericFieldWriter implements NumericFieldWriter {

    private final NumericWriteContext ctx;
    private final Encoder encoder;

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    /**
     * @param ctx     segment-scoped write state shared by every field in this segment
     * @param encoder per-block encoder that supplies the codec-specific numeric encoding
     */
    public TSDBNumericFieldWriter(final NumericWriteContext ctx, final Encoder encoder) {
        this.ctx = ctx;
        this.encoder = encoder;
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            AbstractTSDBDocValuesConsumer.NO_MAX_ORD,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> encoder.encodeBlock(buffer, ctx.blockSize(), data)
        );
    }
}
