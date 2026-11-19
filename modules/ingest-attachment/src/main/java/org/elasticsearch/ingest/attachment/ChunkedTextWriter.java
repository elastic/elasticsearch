/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects Tika's extracted text in fixed-size chunks, so the full text only exists once, as the final {@link String}.
 * <p>
 * Tika's {@code parseToString} grows a {@code StringWriter} (up to 3x the text), copies it into a {@code String}, and the processor copied
 * it again to {@code trim()} it. Here each full chunk becomes a {@code String} (Latin-1 where possible), and {@link #toTrimmedString()}
 * joins them with a single allocation. Not thread safe.
 */
final class ChunkedTextWriter extends Writer {

    static final int DEFAULT_CHUNK_SIZE = 8192;

    private final char[] buffer;
    private int bufferLength;
    private List<String> chunks = new ArrayList<>();
    private long length;

    ChunkedTextWriter() {
        this(DEFAULT_CHUNK_SIZE);
    }

    ChunkedTextWriter(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunk size must be positive but was [" + chunkSize + "]");
        }
        this.buffer = new char[chunkSize];
    }

    @Override
    public void write(char[] cbuf, int off, int len) {
        ensureOpen();
        while (len > 0) {
            int n = Math.min(len, buffer.length - bufferLength);
            System.arraycopy(cbuf, off, buffer, bufferLength, n);
            append(n);
            off += n;
            len -= n;
        }
    }

    @Override
    public void write(String str, int off, int len) {
        ensureOpen();
        while (len > 0) {
            int n = Math.min(len, buffer.length - bufferLength);
            str.getChars(off, off + n, buffer, bufferLength);
            append(n);
            off += n;
            len -= n;
        }
    }

    @Override
    public void write(int c) {
        ensureOpen();
        buffer[bufferLength] = (char) c;
        append(1);
    }

    private void append(int n) {
        bufferLength += n;
        length += n;
        if (bufferLength == buffer.length) {
            sealBuffer();
        }
    }

    private void sealBuffer() {
        if (bufferLength > 0) {
            chunks.add(new String(buffer, 0, bufferLength));
            bufferLength = 0;
        }
    }

    /** Number of chars written so far, untrimmed. */
    long length() {
        return length;
    }

    /**
     * Writes the first {@code maxChars} chars (or all if -1) to {@code out}, chunk by chunk, without copying the whole text.
     */
    void writeTo(Writer out, long maxChars) throws IOException {
        ensureOpen();
        sealBuffer();
        long remaining = maxChars < 0 ? Long.MAX_VALUE : maxChars;
        // the sealed buffer is empty, reuse it to pass on each chunk
        for (int i = 0; i < chunks.size() && remaining > 0; i++) {
            String chunk = chunks.get(i);
            int n = (int) Math.min(chunk.length(), remaining);
            chunk.getChars(0, n, buffer, 0);
            out.write(buffer, 0, n);
            remaining -= n;
        }
    }

    /**
     * Returns the text trimmed exactly like {@link String#trim()} and releases the chunks. The writer can't be used afterwards.
     */
    String toTrimmedString() {
        ensureOpen();
        sealBuffer();
        final List<String> chunks = this.chunks;
        this.chunks = null;

        int first = 0;
        int start = -1;
        for (; first < chunks.size(); first++) {
            start = firstNonWhitespace(chunks.get(first));
            if (start >= 0) {
                break;
            }
        }
        if (start < 0) {
            return "";
        }
        int last = chunks.size() - 1;
        int end = -1;
        for (; last >= first; last--) {
            end = lastNonWhitespace(chunks.get(last));
            if (end >= 0) {
                break;
            }
        }
        assert end >= 0 : "chunk [" + first + "] has a non-whitespace char";
        end++; // exclusive

        if (first == last) {
            return chunks.get(first).substring(start, end);
        }
        String[] parts = new String[last - first + 1];
        parts[0] = chunks.get(first).substring(start);
        for (int i = first + 1; i < last; i++) {
            parts[i - first] = chunks.get(i);
        }
        parts[parts.length - 1] = chunks.get(last).substring(0, end);
        // String#join allocates the result once, without an intermediate copy
        return String.join("", parts);
    }

    /** Index of the first char that {@link String#trim()} keeps, or -1 if it would drop all of them. */
    private static int firstNonWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > ' ') {
                return i;
            }
        }
        return -1;
    }

    /** Index of the last char that {@link String#trim()} keeps, or -1 if it would drop all of them. */
    private static int lastNonWhitespace(String s) {
        for (int i = s.length() - 1; i >= 0; i--) {
            if (s.charAt(i) > ' ') {
                return i;
            }
        }
        return -1;
    }

    private void ensureOpen() {
        if (chunks == null) {
            throw new IllegalStateException("text was already collected");
        }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
}
