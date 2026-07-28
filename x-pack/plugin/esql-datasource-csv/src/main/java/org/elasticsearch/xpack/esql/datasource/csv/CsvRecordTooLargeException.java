/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.io.IOException;

/**
 * Raised when a CSV/TSV record exceeds the configured {@code max_record_size} byte budget. Thrown
 * either by {@link CsvRecordCappingInputStream} (the hot data path, raw-byte and quote-unaware) or
 * by {@link CsvLogicalRecordReader} (legacy bracket-aware path, char-decoded byte estimate).
 */
final class CsvRecordTooLargeException extends IOException {
    CsvRecordTooLargeException(int maxRecordBytes) {
        super("CSV record exceeded max_record_size [" + maxRecordBytes + "]");
    }
}
