/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

/**
 * Marker thrown by the CSV tokenisers (Jackson and the bracket-aware path) when a single
 * row cannot be parsed. The single point of truth for what to do with it lives in
 * {@code CsvFormatReader.CsvBatchIterator.onRowError}, which decides between mapping it to
 * a client {@code ParsingException} (FAIL_FAST) and recording it against the error budget
 * (SKIP_ROW / NULL_FIELD).
 *
 * <p>Kept package-private and unchecked on purpose: it is purely a control-flow signal
 * between the tokeniser and the row-level error policy, never propagated to callers
 * outside this package.
 */
final class MalformedRowException extends RuntimeException {

    MalformedRowException(String message) {
        super(message);
    }

    MalformedRowException(String message, Throwable cause) {
        super(message, cause);
    }
}
