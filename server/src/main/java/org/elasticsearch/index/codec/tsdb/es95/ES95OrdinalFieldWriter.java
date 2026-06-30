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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockWriter;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfigResolver;

import java.io.IOException;

/**
 * {@link OrdinalFieldWriter} implementation for the ES95 TSDB format.
 *
 * <p>{@link #writeFieldEntry} resolves the block size once per field via
 * {@link PipelineConfigResolver}, then writes a per-field {@code blockShift} byte to
 * metadata immediately after the block-layout marker. This lets {@link ES95OrdinalFieldReader}
 * reconstruct the exact block size the field was encoded with, so the ordinal decoder
 * is always sized correctly regardless of the format-level default.
 */
final class ES95OrdinalFieldWriter implements OrdinalFieldWriter {

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();
    private static final FieldContextResolver NO_INFO_RESOLVER = (name, bs) -> new FieldContext(bs, name, null, null, null, false);

    private final NumericWriteContext ctx;
    private final PipelineConfigResolver resolver;
    private final FieldContextResolver fieldContextResolver;

    ES95OrdinalFieldWriter(
        final NumericWriteContext ctx,
        final PipelineConfigResolver resolver,
        @Nullable final FieldContextResolver fieldContextResolver
    ) {
        this.ctx = ctx;
        this.resolver = resolver;
        this.fieldContextResolver = fieldContextResolver != null ? fieldContextResolver : NO_INFO_RESOLVER;
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
        final FieldContext context = fieldContextResolver.resolve(field.name, ctx.blockSize());
        final int blockSize = resolver.resolve(context).blockSize();
        final int blockShift = Integer.numberOfTrailingZeros(blockSize);
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
            () -> ctx.meta().writeByte((byte) blockShift),
            blockSize
        );
    }
}
