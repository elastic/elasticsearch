/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.xpack.esql.datasources.spi.RecordCappingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * NDJSON byte-cap wrapper. Unlike CSV, NDJSON records cannot embed unescaped {@code '\n'} or
 * {@code '\r'}: the JSON grammar requires those control characters to be escaped within string
 * values, so every raw {@code '\n'} or {@code '\r'} byte is unambiguously a record terminator and
 * the cap is precise per line.
 *
 * <p>Wrapped only under strict error policy by {@link NdJsonPageIterator}; the lenient path uses a
 * post-read filter pass to preserve the historical drop-and-continue behavior for oversized lines
 * (a thrown {@link IOException} from this stream is unrecoverable and would turn the row drop into
 * a stream-fatal abort).
 *
 * @see RecordCappingInputStream for the shared CRLF / cap state machine.
 */
final class NdJsonRecordCappingInputStream extends RecordCappingInputStream {

    private final NdJsonRecordSplitter recordSplitter;

    NdJsonRecordCappingInputStream(InputStream in, NdJsonRecordSplitter recordSplitter) {
        super(in, Objects.requireNonNull(recordSplitter, "recordSplitter").maxRecordBytes());
        this.recordSplitter = recordSplitter;
    }

    @Override
    protected IOException recordTooLarge() {
        return recordSplitter.recordTooLargeException();
    }
}
