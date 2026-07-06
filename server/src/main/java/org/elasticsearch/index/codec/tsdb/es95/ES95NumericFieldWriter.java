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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockWriter;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfigResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

import java.io.IOException;

/**
 * {@link NumericFieldWriter} implementation for the ES95 TSDB format.
 *
 * <p>{@link #writeFieldEntry} resolves the pipeline once per field, wraps the resulting
 * {@code NumericEncoder} in an {@link ES95NumericFieldEncoder} that bundles the block-encoding
 * lambda with the {@link org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor}
 * required by the metadata write, and passes both through to {@link TSDBDocValuesBlockWriter}.
 *
 * <p>The constructor accepts an optional {@link FieldContextResolver}; when {@code null}
 * (SPI no-arg codec construction or unit tests that do not exercise mapper-driven
 * selection) a no-info default resolver is substituted, so {@link #writeFieldEntry} can
 * call the resolver unconditionally.
 */
final class ES95NumericFieldWriter implements NumericFieldWriter {

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();
    private static final FieldContextResolver NO_INFO_RESOLVER = (name, bs) -> new FieldContext(bs, name, null, null, null, false);

    private final NumericWriteContext ctx;
    private final PipelineConfigResolver resolver;
    private final FieldContextResolver fieldContextResolver;
    private final NumericCodecFactory numericCodecFactory;

    ES95NumericFieldWriter(
        final NumericWriteContext ctx,
        final PipelineConfigResolver resolver,
        @Nullable final FieldContextResolver fieldContextResolver,
        final NumericCodecFactory numericCodecFactory
    ) {
        this.ctx = ctx;
        this.resolver = resolver;
        this.fieldContextResolver = fieldContextResolver != null ? fieldContextResolver : NO_INFO_RESOLVER;
        this.numericCodecFactory = numericCodecFactory;
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        final FieldContext context = fieldContextResolver.resolve(field.name, ctx.blockSize());
        final PipelineConfig pipelineConfig = resolver.resolve(context);
        final int blockSize = pipelineConfig.blockSize();
        final ES95NumericFieldEncoder numericFieldEncoder = new ES95NumericFieldEncoder(numericCodecFactory.createEncoder(pipelineConfig));
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            AbstractTSDBDocValuesConsumer.NO_MAX_ORD,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> numericFieldEncoder.encodeBlock(buffer, blockSize, data),
            () -> FieldDescriptor.write(ctx.meta(), numericFieldEncoder.descriptor()),
            blockSize
        );
    }
}
