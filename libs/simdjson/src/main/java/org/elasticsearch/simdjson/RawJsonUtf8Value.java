/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.parsers.StringParser;

/**
 * A {@link Utf8Value} for a JSON string that has been neither scanned nor unescaped: only its opening quote and an
 * upper bound on its length are known.
 */
final class RawJsonUtf8Value implements Utf8Value {

    private static final StringParser PARSER = new StringParser();

    private final byte[] buffer;
    private final int quoteIdx;
    private final int maxLength;

    /**
     * @param buffer    the document bytes
     * @param quoteIdx  index of the value's opening quote
     * @param maxLength an upper bound on the unescaped length of the value
     */
    RawJsonUtf8Value(byte[] buffer, int quoteIdx, int maxLength) {
        this.buffer = buffer;
        this.quoteIdx = quoteIdx;
        this.maxLength = maxLength;
    }

    @Override
    public int maxLength() {
        return maxLength;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This is the only pass over the value's bytes: it finds the closing quote and unescapes while copying.
     *
     * @throws JsonParsingException on a malformed escape
     */
    @Override
    public int copyTo(byte[] dst, int dstOffset) {
        return PARSER.parseString(buffer, quoteIdx, dst, dstOffset);
    }
}
