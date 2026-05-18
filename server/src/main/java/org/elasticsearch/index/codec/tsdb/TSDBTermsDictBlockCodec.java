/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

/**
 * Default {@link TermsDictBlockCodec} implementation.
 *
 * <p>Per field, binds the {@code _tsid} field to the raw encoder/decoder pair when
 * {@code skipTsidLz4Encoding} is set in the surrounding context, and binds every other field
 * (and the {@code _tsid} field on legacy segments) to the LZ4 encoder/decoder pair. The chosen
 * encoder or decoder lives for the lifetime of the returned {@link TermsDictFieldReader} or
 * {@link TermsDictFieldWriter}.
 *
 * <p>Each call to {@link #createReader} and {@link #createWriter} returns a fresh per-field
 * instance, mirroring {@link TSDBNumericBlockCodec} and {@link TSDBOrdinalBlockCodec}.
 */
public final class TSDBTermsDictBlockCodec implements TermsDictBlockCodec {

    public TSDBTermsDictBlockCodec() {}

    @Override
    public TermsDictFieldReader createReader(final TermsDictReadContext ctx, final FieldInfo field) {
        return new TSDBTermsDictFieldReader(pickDecoder(ctx.skipTsidLz4Encoding(), field));
    }

    @Override
    public TermsDictFieldWriter createWriter(final TermsDictWriteContext ctx, final FieldInfo field) {
        return new TSDBTermsDictFieldWriter(pickEncoder(ctx.skipTsidLz4Encoding(), field));
    }

    private static TermsDictFieldReader.Decoder pickDecoder(final boolean skipTsidLz4Encoding, final FieldInfo field) {
        if (skipTsidLz4Encoding && TimeSeriesIdFieldMapper.NAME.equals(field.name)) {
            return RawTermsDictDecoder.INSTANCE;
        }
        return LZ4TermsDictDecoder.INSTANCE;
    }

    private static TermsDictFieldWriter.Encoder pickEncoder(final boolean skipTsidLz4Encoding, final FieldInfo field) {
        if (skipTsidLz4Encoding && TimeSeriesIdFieldMapper.NAME.equals(field.name)) {
            return RawTermsDictEncoder.INSTANCE;
        }
        return LZ4TermsDictEncoder.INSTANCE;
    }
}
