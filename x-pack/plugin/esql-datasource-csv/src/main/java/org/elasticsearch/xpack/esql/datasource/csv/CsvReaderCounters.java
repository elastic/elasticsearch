/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe counter struct for {@link CsvFormatReader}; {@link #snapshot()} yields the immutable
 * typed {@link CsvReaderStatus}. The {@code format} field reflects the owning reader's
 * {@link CsvFormatReader#formatName()} so that a CSV instance reports {@code "csv"} and a TSV
 * instance reports {@code "tsv"}.
 */
public final class CsvReaderCounters {

    private final String format;
    private final LongAdder rowsEmitted = new LongAdder();
    private final LongAdder parseErrors = new LongAdder();
    private volatile boolean headerDetected = false;
    private final LongAdder totalReadNanos = new LongAdder();

    public CsvReaderCounters(String format) {
        this.format = format;
    }

    public void addRowsEmitted(long delta) {
        if (delta > 0) {
            rowsEmitted.add(delta);
        }
    }

    public void addParseErrors(long delta) {
        if (delta > 0) {
            parseErrors.add(delta);
        }
    }

    public void markHeaderDetected() {
        headerDetected = true;
    }

    public void addReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    public CsvReaderStatus snapshot() {
        return new CsvReaderStatus(format, rowsEmitted.sum(), parseErrors.sum(), headerDetected, totalReadNanos.sum());
    }
}
