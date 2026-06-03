/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.TSDBOrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.TSDBOrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.TSDBSortedOrdinalBlockCodec;

/**
 * ES95 specialization of {@link TSDBSortedOrdinalBlockCodec} that swaps in the per-block
 * adaptive ordinal encoder for SORTED fields.
 *
 * <p>Overrides {@link #createReader} and {@link #createWriter} to construct an
 * {@link SortedOrdinalCodec} per producer or consumer, so each instance owns its own
 * scratch buffers without shared mutable state.
 */
final class ES95SortedOrdinalBlockCodec extends TSDBSortedOrdinalBlockCodec {

    ES95SortedOrdinalBlockCodec() {}

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        final SortedOrdinalCodec codec = new SortedOrdinalCodec(ctx.blockSize());
        return new TSDBOrdinalFieldReader(codec::decodeOrdinals);
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        final SortedOrdinalCodec codec = new SortedOrdinalCodec(ctx.blockSize());
        return new TSDBOrdinalFieldWriter(ctx, codec::encodeOrdinals);
    }
}
