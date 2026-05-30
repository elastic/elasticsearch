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
import org.elasticsearch.index.codec.tsdb.TSDBSortedSetOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.TupleAwareOrdinalFieldWriter;

/**
 * ES95 specialization of {@link TSDBSortedSetOrdinalBlockCodec} that drives
 * {@link SortedSetOrdinalCodec}. The writer uses {@link TupleAwareOrdinalFieldWriter}
 * so the per-doc value counts and head/tail straddle offsets reach the block encoder,
 * letting {@link TupleRunCodec} group consecutive docs that emit the same K-ord tuple
 * within a {@code _tsid} run.
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
        return new TupleAwareOrdinalFieldWriter(ctx, codec::encodeOrdinals);
    }
}
