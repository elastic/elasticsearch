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
import org.elasticsearch.index.codec.tsdb.TSDBSortedSetOrdinalBlockCodec;

/**
 * ES95 specialization of {@link TSDBSortedSetOrdinalBlockCodec} that swaps in the
 * SORTED_SET adaptive ordinal encoder. Drives {@link SortedSetOrdinalCodec}, which
 * extends the SORTED dispatch with {@link CycleCodec} to absorb the K-cycle pattern
 * produced by multi-valued docs sharing the same ord set within a tsid run.
 */
final class ES95SortedSetOrdinalBlockCodec extends TSDBSortedSetOrdinalBlockCodec {

    ES95SortedSetOrdinalBlockCodec() {}

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        final SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(ctx.blockSize());
        return new TSDBOrdinalFieldReader(codec::decodeOrdinals);
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        final SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(ctx.blockSize());
        return new TSDBOrdinalFieldWriter(ctx, codec::encodeOrdinals);
    }
}
