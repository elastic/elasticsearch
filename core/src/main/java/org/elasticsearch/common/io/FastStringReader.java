/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io;

import java.io.IOException;
import java.io.Reader;

/**
 * A character stream whose source is a string that is <b>not thread safe</b>
 * <p/>
 * (shay.banon
 * )
 */
public class FastStringReader extends CharSequenceReader {

    private String str;
    private int length;
    private int next = 0;
    private int mark = 0;

    /**
     * Creates a new string reader.
     *
     * @param s String providing the character stream.
     */
    public FastStringReader(String s) {
        this.str = s;
        this.length = s.length();
    }

    /**
     * Check to make sure that the stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (length == -1)
            throw new IOException("Stream closed");
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public char charAt(int index) {
        return str.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return str.subSequence(start, end);
    }

    /**
     * Reads a single character.
     *
     * @return The character read, or -1 if the end of the stream has been
     *         reached
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int read() throws IOException {
        ensureOpen();
        if (next >= length)
            return -1;
        return str.charAt(next++);
    }

    /**
     * Reads characters into a portion of an array.
     *
     * @param cbuf Destination buffer
     * @param off  Offset at which to start writing characters
     * @param len  Maximum number of characters to read
     * @return The number of characters read, or -1 if the end of the
     *         stream has been reached
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int read(char cbuf[], int off, int len) throws IOException {
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        if (next >= length)
            return -1;
        int n = Math.min(length - next, len);
        str.getChars(next, next + n, cbuf, off);
        next += n;
        return n;
    }

    /**
     * Skips the specified number of characters in the stream. Returns
     * the number of characters that were skipped.
     * <p/>
     * <p>The <code>ns</code> parameter may be negative, even though the
     * <code>skip</code> method of the {@link Reader} superclass throws
     * an exception in this case. Negative values of <code>ns</code> cause the
     * stream to skip backwards. Negative return values indicate a skip
     * backwards. It is not possible to skip backwards past the beginning of
     * the string.
     * <p/>
     * <p>If the entire string has been read or skipped, then this method has
     * no effect and always returns 0.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public long skip(long ns) throws IOException {
        ensureOpen();
        if (next >= length)
            return 0;
        // Bound skip by beginning and end of the source
        long n = Math.min(length - next, ns);
        n = Math.max(-next, n);
        next += n;
        return n;
    }

    /**
     * Tells whether this stream is ready to be read.
     *
     * @return True if the next read() is guaranteed not to block for input
     * @throws IOException If the stream is closed
     */
    @Override
    public boolean ready() throws IOException {
        ensureOpen();
        return true;
    }

    /**
     * Tells whether this stream supports the mark() operation, which it does.
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Marks the present position in the stream.  Subsequent calls to reset()
     * will reposition the stream to this point.
     *
     * @param readAheadLimit Limit on the number of characters that may be
     *                       read while still preserving the mark.  Because
     *                       the stream's input comes from a string, there
     *                       is no actual limit, so this argument must not
     *                       be negative, but is otherwise ignored.
     * @throws IllegalArgumentException If readAheadLimit is < 0
     * @throws IOException              If an I/O error occurs
     */
    @Override
    public void mark(int readAheadLimit) throws IOException {
        if (readAheadLimit < 0) {
            throw new IllegalArgumentException("Read-ahead limit < 0");
        }
        ensureOpen();
        mark = next;
    }

    /**
     * Resets the stream to the most recent mark, or to the beginning of the
     * string if it has never been marked.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void reset() throws IOException {
        ensureOpen();
        next = mark;
    }

    /**
     * Closes the stream and releases any system resources associated with
     * it. Once the stream has been closed, further read(),
     * ready(), mark(), or reset() invocations will throw an IOException.
     * Closing a previously closed stream has no effect.
     */
    @Override
    public void close() {
        length = -1;
    }

    @Override
    public String toString() {
        return str;
    }
}
