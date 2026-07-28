/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.xpack.esql.datasources.spi.RecordCappingInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * CSV/TSV byte-cap wrapper. Quote-unaware (an embedded {@code '\n'} or {@code '\r'} inside a quoted
 * field resets the counter prematurely), so the cap is a defense-in-depth bound rather than an
 * exact per-record check; the splitter applies the authoritative quote-aware check at split
 * discovery, and Jackson's {@code StreamReadConstraints.maxStringLength} bounds individual fields.
 *
 * <p>Wrapped only on the non-bracket-aware data path; the bracket-aware path uses
 * {@link CsvLogicalRecordReader}'s char-decoded {@code addBytes} cap, which is exact and supports
 * row-level recovery under lenient error policies.
 *
 * <p>Under lenient error policies on the wrapped non-bracket-aware path, a thrown
 * {@link CsvRecordTooLargeException} surfaces as a stream-fatal abort because the underlying
 * stream position is undefined after the trip; the cause chain still carries the original
 * {@code "max_record_size [N]"} message.
 *
 * @see RecordCappingInputStream for the shared CRLF / cap state machine.
 */
final class CsvRecordCappingInputStream extends RecordCappingInputStream {

    private final int maxRecordBytes;

    CsvRecordCappingInputStream(InputStream in, int maxRecordBytes) {
        super(in, maxRecordBytes);
        this.maxRecordBytes = maxRecordBytes;
    }

    @Override
    protected IOException recordTooLarge() {
        return new CsvRecordTooLargeException(maxRecordBytes);
    }
}
