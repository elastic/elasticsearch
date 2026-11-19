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
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;

/**
 * {@link OrdinalBlockCodec} for the ES95 TSDB format. The writer encodes ordinals at the
 * format-level block size and writes no per-field metadata; the reader is version-aware so it can
 * still read the per-field {@code blockShift} byte written by earlier binaries.
 */
final class ES95OrdinalCodec implements OrdinalBlockCodec {

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        return new ES95OrdinalFieldReader(ctx.segmentVersion());
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        return new ES95OrdinalFieldWriter(ctx);
    }
}
