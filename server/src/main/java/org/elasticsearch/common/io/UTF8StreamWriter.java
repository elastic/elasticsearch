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

import java.io.CharConversionException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;


public final class UTF8StreamWriter extends Writer {

    /**
     * Holds the current output stream or <code>null</code> if closed.
     */
    private OutputStream _outputStream;

    /**
     * Holds the bytes' buffer.
     */
    private final byte[] _bytes;

    /**
     * Holds the bytes buffer index.
     */
    private int _index;

    /**
     * Creates a UTF-8 writer having a byte buffer of moderate capacity (2048).
     */
    public UTF8StreamWriter() {
        _bytes = new byte[2048];
    }

    /**
     * Creates a UTF-8 writer having a byte buffer of specified capacity.
     *
     * @param capacity the capacity of the byte buffer.
     */
    public UTF8StreamWriter(int capacity) {
        _bytes = new byte[capacity];
    }

    /**
     * Sets the output stream to use for writing until this writer is closed.
     * For example:[code]
     * Writer writer = new UTF8StreamWriter().setOutputStream(out);
     * [/code] is equivalent but writes faster than [code]
     * Writer writer = new java.io.OutputStreamWriter(out, "UTF-8");
     * [/code]
     *
     * @param out the output stream.
     * @return this UTF-8 writer.
     * @throws IllegalStateException if this writer is being reused and
     *                               it has not been {@link #close closed} or {@link #reset reset}.
     */
    public UTF8StreamWriter setOutput(OutputStream out) {
        if (_outputStream != null)
            throw new IllegalStateException("Writer not closed or reset");
        _outputStream = out;
        return this;
    }

    /**
     * Writes a single character. This method supports 16-bits
     * character surrogates.
     *
     * @param c <code>char</code> the character to be written (possibly
     *          a surrogate).
     * @throws IOException if an I/O error occurs.
     */
    public void write(char c) throws IOException {
        if ((c < 0xd800) || (c > 0xdfff)) {
            write((int) c);
        } else if (c < 0xdc00) { // High surrogate.
            _highSurrogate = c;
        } else { // Low surrogate.
            int code = ((_highSurrogate - 0xd800) << 10) + (c - 0xdc00)
                    + 0x10000;
            write(code);
        }
    }

    private char _highSurrogate;

    /**
     * Writes a character given its 31-bits Unicode.
     *
     * @param code the 31 bits Unicode of the character to be written.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void write(int code) throws IOException {
        if ((code & 0xffffff80) == 0) {
            _bytes[_index] = (byte) code;
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else { // Writes more than one byte.
            write2(code);
        }
    }

    private void write2(int c) throws IOException {
        if ((c & 0xfffff800) == 0) { // 2 bytes.
            _bytes[_index] = (byte) (0xc0 | (c >> 6));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | (c & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else if ((c & 0xffff0000) == 0) { // 3 bytes.
            _bytes[_index] = (byte) (0xe0 | (c >> 12));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 6) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | (c & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else if ((c & 0xff200000) == 0) { // 4 bytes.
            _bytes[_index] = (byte) (0xf0 | (c >> 18));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 12) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 6) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | (c & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else if ((c & 0xf4000000) == 0) { // 5 bytes.
            _bytes[_index] = (byte) (0xf8 | (c >> 24));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 18) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 12) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 6) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | (c & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else if ((c & 0x80000000) == 0) { // 6 bytes.
            _bytes[_index] = (byte) (0xfc | (c >> 30));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 24) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 18) & 0x3f));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 12) & 0x3F));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | ((c >> 6) & 0x3F));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
            _bytes[_index] = (byte) (0x80 | (c & 0x3F));
            if (++_index >= _bytes.length) {
                flushBuffer();
            }
        } else {
            throw new CharConversionException("Illegal character U+"
                    + Integer.toHexString(c));
        }
    }

    /**
     * Writes a portion of an array of characters.
     *
     * @param cbuf the array of characters.
     * @param off  the offset from which to start writing characters.
     * @param len  the number of characters to write.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void write(char cbuf[], int off, int len) throws IOException {
        final int off_plus_len = off + len;
        for (int i = off; i < off_plus_len; ) {
            char c = cbuf[i++];
            if (c < 0x80) {
                _bytes[_index] = (byte) c;
                if (++_index >= _bytes.length) {
                    flushBuffer();
                }
            } else {
                write(c);
            }
        }
    }

    /**
     * Writes a portion of a string.
     *
     * @param str a String.
     * @param off the offset from which to start writing characters.
     * @param len the number of characters to write.
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(String str, int off, int len) throws IOException {
        final int off_plus_len = off + len;
        for (int i = off; i < off_plus_len; ) {
            char c = str.charAt(i++);
            if (c < 0x80) {
                _bytes[_index] = (byte) c;
                if (++_index >= _bytes.length) {
                    flushBuffer();
                }
            } else {
                write(c);
            }
        }
    }

    /**
     * Writes the specified character sequence.
     *
     * @param csq the character sequence.
     * @throws IOException if an I/O error occurs
     */
    public void write(CharSequence csq) throws IOException {
        final int length = csq.length();
        for (int i = 0; i < length; ) {
            char c = csq.charAt(i++);
            if (c < 0x80) {
                _bytes[_index] = (byte) c;
                if (++_index >= _bytes.length) {
                    flushBuffer();
                }
            } else {
                write(c);
            }
        }
    }

    /**
     * Flushes the stream.  If the stream has saved any characters from the
     * various write() methods in a buffer, write them immediately to their
     * intended destination.  Then, if that destination is another character or
     * byte stream, flush it.  Thus one flush() invocation will flush all the
     * buffers in a chain of Writers and OutputStreams.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void flush() throws IOException {
        flushBuffer();
        _outputStream.flush();
    }

    /**
     * Closes and {@link #reset resets} this writer for reuse.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (_outputStream != null) {
            flushBuffer();
            _outputStream.close();
            reset();
        }
    }

    /**
     * Flushes the internal bytes buffer.
     *
     * @throws IOException if an I/O error occurs
     */
    private void flushBuffer() throws IOException {
        if (_outputStream == null)
            throw new IOException("Stream closed");
        _outputStream.write(_bytes, 0, _index);
        _index = 0;
    }

    // Implements Reusable.
    public void reset() {
        _highSurrogate = 0;
        _index = 0;
        _outputStream = null;
    }
}