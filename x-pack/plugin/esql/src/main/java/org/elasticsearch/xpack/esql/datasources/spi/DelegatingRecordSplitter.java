/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

final class DelegatingRecordSplitter implements RecordSplitter {

    private final SegmentableFormatReader reader;

    DelegatingRecordSplitter(SegmentableFormatReader reader) {
        this.reader = Objects.requireNonNull(reader);
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        long consumed = reader.findNextRecordBoundary(stream);
        assert consumed != RECORD_TOO_LARGE : "SegmentableFormatReader cannot report RecordSplitter.RECORD_TOO_LARGE";
        return consumed;
    }

    @Override
    public int findLastRecordBoundary(byte[] buf, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, buf.length);
        // The legacy reader method is offset-unaware; copy only in the bridge path that adapts it.
        int boundary = offset == 0
            ? reader.findLastRecordBoundary(buf, length)
            : reader.findLastRecordBoundary(Arrays.copyOfRange(buf, offset, offset + length), length);
        assert boundary != RECORD_TOO_LARGE : "SegmentableFormatReader cannot report RecordSplitter.RECORD_TOO_LARGE";
        return boundary < 0 || offset == 0 ? boundary : offset + boundary;
    }

    @Override
    public int maxRecordBytes() {
        return SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES;
    }
}
