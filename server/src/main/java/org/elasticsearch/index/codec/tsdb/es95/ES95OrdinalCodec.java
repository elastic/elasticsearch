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
 * {@link OrdinalBlockCodec} for the ES95 TSDB format.
 *
 * <p>Creates {@link ES95OrdinalFieldWriter} and {@link ES95OrdinalFieldReader} instances.
 * Ordinal fields use the format-level block size and carry no per-field block metadata.
 */
final class ES95OrdinalCodec implements OrdinalBlockCodec {

    @Override
    public OrdinalFieldReader createReader(final NumericReadContext ctx) {
        return new ES95OrdinalFieldReader();
    }

    @Override
    public OrdinalFieldWriter createWriter(final NumericWriteContext ctx) {
        return new ES95OrdinalFieldWriter(ctx);
    }
}
