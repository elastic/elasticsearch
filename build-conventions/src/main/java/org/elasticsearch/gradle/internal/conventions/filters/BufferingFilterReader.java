/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.filters;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Base class for copy filters that need to see the whole content at once. The underlying reader is consumed on first
 * read, transformed with {@link #transform(String)} and served from memory. Intended for the small text files
 * (scripts, configuration, test resources) that the build filters while copying.
 */
abstract class BufferingFilterReader extends FilterReader {

    private Reader transformed;

    protected BufferingFilterReader(Reader in) {
        super(in);
    }

    /**
     * Transforms the full content of the underlying reader.
     */
    protected abstract String transform(String content);

    private Reader transformed() throws IOException {
        if (transformed == null) {
            StringWriter content = new StringWriter();
            in.transferTo(content);
            transformed = new StringReader(transform(content.toString()));
        }
        return transformed;
    }

    @Override
    public int read() throws IOException {
        return transformed().read();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return transformed().read(cbuf, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return transformed().skip(n);
    }

    @Override
    public boolean ready() throws IOException {
        return transformed().ready();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        throw new IOException("mark() not supported");
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("reset() not supported");
    }
}
