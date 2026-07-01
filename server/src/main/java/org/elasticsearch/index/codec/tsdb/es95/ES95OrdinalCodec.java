/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfigResolver;

/**
 * {@link OrdinalBlockCodec} for the ES95 TSDB format.
 *
 * <p>Creates {@link ES95OrdinalFieldWriter} and {@link ES95OrdinalFieldReader} instances.
 * The writer carries a per-field {@code blockShift} byte in segment metadata so the decoder
 * always uses the exact block size the field was encoded with.
 */
final class ES95OrdinalCodec implements OrdinalBlockCodec {

    private final PipelineConfigResolver resolver;
    @Nullable
    private final FieldContextResolver fieldContextResolver;

    ES95OrdinalCodec(final PipelineConfigResolver resolver, @Nullable final FieldContextResolver fieldContextResolver) {
        this.resolver = resolver;
        this.fieldContextResolver = fieldContextResolver;
    }

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        return new ES95OrdinalFieldReader(ctx.segmentVersion());
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        return new ES95OrdinalFieldWriter(ctx, resolver, fieldContextResolver);
    }
}
