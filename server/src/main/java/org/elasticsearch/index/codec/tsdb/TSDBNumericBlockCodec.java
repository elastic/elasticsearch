/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * {@link NumericBlockCodec} that encodes numeric value blocks with {@link TSDBDocValuesEncoder}.
 *
 * <p>Each call to {@link #createReader} and {@link #createWriter} returns a fresh instance
 * so that each producer or consumer owns its own encoder buffers without shared mutable state.
 */
public final class TSDBNumericBlockCodec implements NumericBlockCodec {

    /** Creates a new codec that uses {@link TSDBDocValuesEncoder} for block encoding. */
    public TSDBNumericBlockCodec() {}

    @Override
    public NumericFieldReader createReader(final NumericReadContext ctx) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ctx.blockSize());
        return new TSDBNumericFieldReader((input, values, count) -> encoder.decode(input, values));
    }

    @Override
    public NumericFieldWriter createWriter(final NumericWriteContext ctx) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ctx.blockSize());
        return new TSDBNumericFieldWriter(ctx, (values, blockSize, data) -> encoder.encode(values, data));
    }
}
