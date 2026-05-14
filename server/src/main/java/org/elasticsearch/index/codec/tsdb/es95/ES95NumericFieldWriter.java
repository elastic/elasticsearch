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
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockWriter;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

import java.io.IOException;

/**
 * {@link NumericFieldWriter} implementation for the ES95 TSDB format.
 *
 * <p>{@link #encoder(FieldContext)} is the single entry point for per-field setup: it
 * resolves the pipeline once and returns an {@link ES95NumericFieldEncoder} that exposes both the
 * block-encoding lambda and the {@link org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor}
 * required by the metadata write. {@link #writeFieldEntry} calls {@code encoder()} and
 * passes the resulting per-field bundle through to {@link TSDBDocValuesBlockWriter}.
 */
final class ES95NumericFieldWriter implements NumericFieldWriter {

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    private final NumericWriteContext ctx;
    private final PipelineResolver pipelineResolver;
    private final NumericCodecFactory numericCodecFactory;

    ES95NumericFieldWriter(
        final NumericWriteContext ctx,
        final PipelineResolver pipelineResolver,
        final NumericCodecFactory numericCodecFactory
    ) {
        this.ctx = ctx;
        this.pipelineResolver = pipelineResolver;
        this.numericCodecFactory = numericCodecFactory;
    }

    @Override
    public ES95NumericFieldEncoder encoder(final FieldContext context) {
        return new ES95NumericFieldEncoder(numericCodecFactory.createEncoder(pipelineResolver.resolve(context)));
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        final ES95NumericFieldEncoder numericFieldEncoder = encoder(new FieldContext(ctx.blockSize(), field.name));
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            AbstractTSDBDocValuesConsumer.NO_MAX_ORD,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> numericFieldEncoder.encodeBlock(buffer, buffer.length, data),
            () -> FieldDescriptor.write(ctx.meta(), numericFieldEncoder.descriptor())
        );
    }

}
