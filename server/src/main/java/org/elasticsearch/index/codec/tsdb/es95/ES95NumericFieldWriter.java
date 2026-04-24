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
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;

/**
 * {@link NumericFieldWriter} implementation for the ES95 TSDB format.
 *
 * <p>{@link #writeFieldEntry} delegates to {@link TSDBDocValuesBlockWriter} for the shared
 * block layout and additionally writes a {@link FieldDescriptor} to the field's metadata
 * header so decoders can reconstruct the pipeline from segment metadata.
 *
 * <p>{@link #encoder()} returns an {@link Encoder} backed by the ES95 pipeline
 * ({@code delta > offset > gcd > bitpack}).
 */
final class ES95NumericFieldWriter implements NumericFieldWriter {

    private static final TSDBDocValuesBlockWriter BLOCK_WRITER = new TSDBDocValuesBlockWriter();

    private final NumericWriteContext ctx;
    private final PipelineConfigFactory pipelineConfigFactory;
    private final NumericCodecFactory numericCodecFactory;

    ES95NumericFieldWriter(
        final NumericWriteContext ctx,
        final PipelineConfigFactory pipelineConfigFactory,
        final NumericCodecFactory numericCodecFactory
    ) {
        this.ctx = ctx;
        this.pipelineConfigFactory = pipelineConfigFactory;
        this.numericCodecFactory = numericCodecFactory;
    }

    @Override
    public Encoder encoder() {
        final EncodingPipeline state = createPipeline();
        return (values, blockSize, data) -> state.blockEncoder.encode(values, blockSize, data);
    }

    @Override
    public DocValueFieldCountStats writeFieldEntry(
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        final AbstractTSDBDocValuesConsumer.DocValueCountConsumer docValueCountConsumer,
        final SortedFieldObserver sortedFieldObserver
    ) throws IOException {
        final EncodingPipeline state = createPipeline();
        return BLOCK_WRITER.writeFieldEntry(
            ctx,
            field,
            valuesSource,
            AbstractTSDBDocValuesConsumer.NO_MAX_ORD,
            docValueCountConsumer,
            sortedFieldObserver,
            (buffer, data) -> state.blockEncoder.encode(buffer, buffer.length, data),
            () -> FieldDescriptor.write(ctx.meta(), state.descriptor)
        );
    }

    private EncodingPipeline createPipeline() {
        final NumericEncoder encoder = numericCodecFactory.createEncoder(pipelineConfigFactory.create(ctx.blockSize()));
        return new EncodingPipeline(encoder.newBlockEncoder(), encoder.descriptor());
    }

    /** Pairs the block encoder with its pipeline descriptor so the descriptor can be written to metadata. */
    private record EncodingPipeline(NumericBlockEncoder blockEncoder, PipelineDescriptor descriptor) {}
}
