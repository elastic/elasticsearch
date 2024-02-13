/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import org.elasticsearch.core.SuppressForbidden;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Map;

/**
 * {@link PrintWriter} formatting lines as JSON using a subset of fields written by ECSJsonLayout.
 *
 * When using {@code println}, anything that looks like a JSON object is immediately passed through and written as is.
 */
public class JsonPrintWriter extends PrintWriter {
    private static final DateTimeFormatter ISO_INSTANT_FORMATTER = new DateTimeFormatterBuilder().appendInstant(3).toFormatter(Locale.ROOT);
    private static final String QUOTED_LINE_SEPARATOR = JsonUtils.quoteAsString(System.getProperty("line.separator"));
    static final int BUFFER_INITIAL_SIZE = 1024;
    static final int BUFFER_TRIM_THRESHOLD = 2 * BUFFER_INITIAL_SIZE;

    // Buffer to build JSON mimicking the ECSJsonLayout.
    // NEVER acquire monitor lock on `lock` if holding `buffer`, always acquire `lock` first!
    private final StringBuilder buffer;
    private final int bufferPreambleSize;
    private final Clock clock;

    private boolean singleLineMode = false;

    public JsonPrintWriter(Map<String, String> staticFields, OutputStream out, boolean autoFlush) {
        this(staticFields, out, autoFlush, Clock.systemUTC());
    }

    @SuppressForbidden(reason = "Override PrintWriter to emit Json")
    protected JsonPrintWriter(Map<String, String> staticFields, OutputStream out, boolean autoFlush, Clock clock) {
        super(out, autoFlush);
        this.clock = clock;
        this.buffer = new StringBuilder(BUFFER_INITIAL_SIZE);
        this.bufferPreambleSize = initStaticPreamble(buffer, staticFields);
    }

    private static int initStaticPreamble(StringBuilder builder, Map<String, String> fields) {
        builder.append('{');
        fields.forEach((name, value) -> builder.append('"').append(name).append("\":\"").append(value).append("\","));
        builder.append("\"@timestamp\":\"");
        return builder.length();
    }

    private void initMessageIfAtPreamble() {
        assert Thread.holdsLock(buffer); // only allow if holding lock on buffer
        assert buffer.length() >= bufferPreambleSize;

        if (buffer.length() == bufferPreambleSize) {
            ISO_INSTANT_FORMATTER.formatTo(clock.instant(), buffer);
            buffer.append("\", \"message\":\"");
        }
    }

    @Override
    public void write(int c) {
        synchronized (buffer) {
            initMessageIfAtPreamble();
            JsonUtils.quote((char) c, buffer);
        }
    }

    @Override
    public void write(char[] chars, int off, int len) {
        synchronized (buffer) {
            initMessageIfAtPreamble();
            JsonUtils.quoteAsString(chars, off, len, buffer);
        }
    }

    @Override
    public void write(String msg, int off, int len) {
        synchronized (buffer) {
            initMessageIfAtPreamble();
            JsonUtils.quoteAsString(msg, off, len, buffer);
        }
    }

    @Override
    public void println(String msg) {
        synchronized (lock) {
            if (singleLineMode == false && isJsonObject(msg)) {
                super.write(msg, 0, msg.length());
                super.println();
            } else {
                synchronized (buffer) {
                    write(msg, 0, msg.length());
                    println();
                }
            }
        }
    }

    @Override
    public void println(char[] chars) {
        synchronized (lock) {
            if (singleLineMode == false && isJsonObject(chars)) {
                // Forward as is, ignore current buffer.
                super.write(chars, 0, chars.length);
                super.println();
            } else {
                synchronized (buffer) {
                    write(chars, 0, chars.length);
                    println();
                }
            }
        }
    }

    /** Print a throwable (with stacktrace) into a single JSON object. */
    public void println(Throwable throwable) {
        synchronized (lock) {
            synchronized (buffer) {
                singleLineMode = true;
                try {
                    write(throwable.getMessage());
                    buffer.append("\",\"error.type\":\"").append(throwable.getClass().getName());
                    buffer.append("\",\"error.stack_trace\":\"");
                    throwable.printStackTrace(this);
                } finally {
                    singleLineMode = false;
                }
                println();
            }
        }
    }

    @Override
    public void println(Object x) {
        println(String.valueOf(x));
    }

    @Override
    public void println() {
        if (singleLineMode) {
            write(System.lineSeparator()); // will be quoted!
        } else {
            synchronized (lock) {
                writeBufferedJson();
                super.println();
            }
        }
    }

    @Override
    public void flush() {
        writeBufferedJson();
        super.flush();
    }

    @Override
    public void close() {
        flush();
        super.close();
    }

    private void writeBufferedJson() {
        String json = null;
        boolean newline = false;
        synchronized (buffer) {
            if (buffer.length() > bufferPreambleSize) {
                // properly handle trailing newlines if not emitted using println()
                newline = stripSuffix(buffer, QUOTED_LINE_SEPARATOR);
                json = buffer.append("\"}").toString();
                if (buffer.capacity() > BUFFER_TRIM_THRESHOLD) {
                    buffer.setLength(buffer.capacity() >> 1); // reduce capacity again by half
                    buffer.trimToSize();
                }
                buffer.setLength(bufferPreambleSize); // reset to preamble
            }
        }
        if (json != null) {
            super.write(json, 0, json.length());
            if (newline) {
                super.println();
            }
        }
    }

    // Visible for testing
    static boolean stripSuffix(StringBuilder builder, String suffix) {
        int strippedLength = builder.length() - suffix.length();
        if (strippedLength >= 0 && builder.lastIndexOf(suffix, strippedLength) == strippedLength) {
            builder.setLength(strippedLength);
            return true;
        }
        return false;
    }

    /**
     * Checks if {@code str} is likely a JSON object starting with `{` without any further validation.
     */
    protected static boolean isJsonObject(String str) {
        return str != null && str.length() > 0 && str.charAt(0) == '{';
    }

    /**
     * Checks if {@code chars} is likely a JSON object starting with `{` without any further validation.
     */
    protected static boolean isJsonObject(char[] chars) {
        return chars != null && chars.length > 0 && chars[0] == '{';
    }
}
