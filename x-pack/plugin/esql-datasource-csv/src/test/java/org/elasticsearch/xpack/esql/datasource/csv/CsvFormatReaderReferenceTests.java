/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

/**
 * Re-runs the entire {@link CsvFormatReaderTests} suite against
 * {@link CsvBoundaryScanner#REFERENCE} — the per-byte scalar scanner — instead of the prefix-XOR
 * fast path the base class uses by default. Together these two suites enforce that both
 * implementations behave byte-for-byte identically end-to-end through the public reader API.
 */
public class CsvFormatReaderReferenceTests extends CsvFormatReaderTests {

    @Override
    protected CsvBoundaryScanner.Impl scannerImpl() {
        return CsvBoundaryScanner.REFERENCE;
    }
}
