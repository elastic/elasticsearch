/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.io;

import org.elasticsearch.util.concurrent.NotThreadSafe;

import java.io.Writer;

/**
 * A Writer based on {@link StringBuilder}. Also alows for thread local reuse of {@link StringBuilder}
 * by using: <code>StringBuilderWriter.Cached.cached()</code> in order to obtain the cached writer. Note,
 * in such cases, the {@link #getBuilder()} should be called and used (usually <code>toString</code> it)
 * before another usage of the writer.
 *
 * @author kimchy (Shay Banon)
 */
@NotThreadSafe
public class StringBuilderWriter extends Writer {

    /**
     * A thread local based cache of {@link StringBuilderWriter}.
     */
    public static class Cached {

        private static final ThreadLocal<StringBuilderWriter> cache = new ThreadLocal<StringBuilderWriter>() {
            @Override protected StringBuilderWriter initialValue() {
                return new StringBuilderWriter();
            }
        };

        /**
         * Returns the cached thread local writer, with its internal {@link StringBuilder} cleared.
         */
        public static StringBuilderWriter cached() {
            StringBuilderWriter writer = cache.get();
            writer.getBuilder().setLength(0);
            return writer;
        }
    }

    private final StringBuilder builder;

    /**
     * Construct a new {@link StringBuilder} instance with default capacity.
     */
    public StringBuilderWriter() {
        this.builder = new StringBuilder();
    }

    /**
     * Construct a new {@link StringBuilder} instance with the specified capacity.
     *
     * @param capacity The initial capacity of the underlying {@link StringBuilder}
     */
    public StringBuilderWriter(int capacity) {
        this.builder = new StringBuilder(capacity);
    }

    /**
     * Construct a new instance with the specified {@link StringBuilder}.
     *
     * @param builder The String builder
     */
    public StringBuilderWriter(StringBuilder builder) {
        this.builder = (builder != null ? builder : new StringBuilder());
    }

    /**
     * Append a single character to this Writer.
     *
     * @param value The character to append
     * @return This writer instance
     */
    public Writer append(char value) {
        builder.append(value);
        return this;
    }

    /**
     * Append a character sequence to this Writer.
     *
     * @param value The character to append
     * @return This writer instance
     */
    public Writer append(CharSequence value) {
        builder.append(value);
        return this;
    }

    /**
     * Append a portion of a character sequence to the {@link StringBuilder}.
     *
     * @param value The character to append
     * @param start The index of the first character
     * @param end   The index of the last character + 1
     * @return This writer instance
     */
    public Writer append(CharSequence value, int start, int end) {
        builder.append(value, start, end);
        return this;
    }

    /**
     * Closing this writer has no effect.
     */
    public void close() {
    }

    /**
     * Flushing this writer has no effect.
     */
    public void flush() {
    }


    /**
     * Write a String to the {@link StringBuilder}.
     *
     * @param value The value to write
     */
    public void write(String value) {
        if (value != null) {
            builder.append(value);
        }
    }

    /**
     * Write a portion of a character array to the {@link StringBuilder}.
     *
     * @param value  The value to write
     * @param offset The index of the first character
     * @param length The number of characters to write
     */
    public void write(char[] value, int offset, int length) {
        if (value != null) {
            builder.append(value, offset, length);
        }
    }

    /**
     * Return the underlying builder.
     *
     * @return The underlying builder
     */
    public StringBuilder getBuilder() {
        return builder;
    }

    /**
     * Returns {@link StringBuilder#toString()}.
     *
     * @return The contents of the String builder.
     */
    public String toString() {
        return builder.toString();
    }
}
