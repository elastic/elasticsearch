/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.NumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.NumericFieldReader;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfigResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * {@link NumericBlockCodec} that encodes numeric value blocks via the ES95 pipeline.
 *
 * <p>Each call to {@link #createReader} and {@link #createWriter} returns a fresh
 * {@link ES95NumericFieldReader} or {@link ES95NumericFieldWriter} instance so that
 * each producer or consumer owns its own encoder state without shared mutable state.
 *
 * <p>Each field writes a self-describing
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor} so decoders
 * reconstruct themselves from segment metadata.
 */
final class ES95NumericCodec implements NumericBlockCodec {

    private final PipelineConfigResolver resolver;
    private final NumericCodecFactory numericCodecFactory;
    private final FallbackDecoderFactory fallbackDecoderFactory;

    ES95NumericCodec(
        final PipelineConfigResolver resolver,
        final NumericCodecFactory numericCodecFactory,
        final FallbackDecoderFactory fallbackDecoderFactory
    ) {
        this.resolver = resolver;
        this.numericCodecFactory = numericCodecFactory;
        this.fallbackDecoderFactory = fallbackDecoderFactory;
    }

    @Override
    public NumericFieldReader createReader(final NumericReadContext ctx) {
        return new ES95NumericFieldReader(numericCodecFactory, fallbackDecoderFactory.create(ctx.blockSize()));
    }

    @Override
    public NumericFieldWriter createWriter(final NumericWriteContext ctx) {
        return new ES95NumericFieldWriter(ctx, resolver, numericCodecFactory);
    }
}
