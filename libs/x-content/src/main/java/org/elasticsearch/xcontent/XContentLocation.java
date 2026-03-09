/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

/**
 * Simple data structure representing the line and column number of a position
 * in some XContent e.g. JSON. Locations are typically used to communicate the
 * position of a parsing error to end users and consequently have line and
 * column numbers starting from 1.
 *
 * <p>The optional {@code byteOffset} field holds the absolute byte position
 * within the source stream ({@code -1} when not available). Byte offsets are
 * used for programmatic byte-range slicing and are not included in the
 * human-readable {@link #toString()} output.
 */
public record XContentLocation(int lineNumber, int columnNumber, long byteOffset) {

    public static final XContentLocation UNKNOWN = new XContentLocation(-1, -1, -1L);

    /**
     * Backward-compatible constructor that sets {@code byteOffset} to {@code -1}
     * (not available).
     */
    public XContentLocation(int lineNumber, int columnNumber) {
        this(lineNumber, columnNumber, -1L);
    }

    /** Returns {@code true} if the line number is valid (1-based, so must be &ge; 1). */
    public boolean hasValidLineNumber() {
        return lineNumber >= 1;
    }

    /** Returns {@code true} if the column number is valid (1-based, so must be &ge; 1). */
    public boolean hasValidColumnNumber() {
        return columnNumber >= 1;
    }

    /** Returns {@code true} if the byte offset is available (non-negative). */
    public boolean hasValidByteOffset() {
        return byteOffset >= 0;
    }

    @Override
    public String toString() {
        return lineNumber + ":" + columnNumber;
    }
}
