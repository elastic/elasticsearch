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
 * Baseline {@link SortedOrdinalBlockCodec} that encodes SORTED ordinal value blocks with
 * {@link TSDBDocValuesEncoder}.
 *
 * <p>Each call to {@link #createReader} and {@link #createWriter} returns a fresh instance so
 * that each producer or consumer owns its own encoder buffers without shared mutable state.
 * Subclasses may override either method to swap in a specialized block encoder while keeping
 * the field-level layout unchanged.
 */
public class TSDBSortedOrdinalBlockCodec implements SortedOrdinalBlockCodec {

    /** Creates a new codec that uses {@link TSDBDocValuesEncoder} for block encoding. */
    public TSDBSortedOrdinalBlockCodec() {}

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ctx.blockSize());
        return new TSDBOrdinalFieldReader(encoder::decodeOrdinals);
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ctx.blockSize());
        return new TSDBOrdinalFieldWriter(ctx, encoder::encodeOrdinals);
    }
}
