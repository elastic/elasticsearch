/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import java.util.Locale;

/**
 * Exception thrown when there is a potential data loss during parsing.
 * This indicates that the actual length of the resulting data (pattern and arguments) does not match the expected length,
 * based on the input message.
 */
public class DataLossParseException extends ParseException {

    private final int expectedLength;
    private final int actualLength;

    public DataLossParseException(String message, int expectedLength, int actualLength) {
        super(String.format(Locale.ROOT, "%s (expected length: %d, actual length: %d)", message, expectedLength, actualLength));
        this.expectedLength = expectedLength;
        this.actualLength = actualLength;
    }

    public int getExpectedLength() {
        return expectedLength;
    }

    public int getActualLength() {
        return actualLength;
    }
}
