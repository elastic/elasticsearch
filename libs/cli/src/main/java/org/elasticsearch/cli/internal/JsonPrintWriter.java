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

/**
 * {@link PrintWriter} formatting lines as JSON using a subset of fields written by ECSJsonLayout.
 *
 * When using {@code println}, anything that looks like a JSON object is immediately passed through and written as is.
 */
public class JsonPrintWriter extends PrintWriter {
    private static final DateTimeFormatter ISO_INSTANT_FORMATTER = new DateTimeFormatterBuilder().appendInstant(3).toFormatter(Locale.ROOT);
    static final int BUFFER_INITIAL_SIZE = 512;
    static final int BUFFER_TRIM_THRESHOLD = 2 * BUFFER_INITIAL_SIZE;

    // Buffer to build JSON mimicking the ECSJsonLayout.
    // NEVER acquire monitor lock on `lock` if holding `buffer`, always acquire `lock` first!
    private final StringBuilder buffer = new StringBuilder(BUFFER_INITIAL_SIZE);
    private final Clock clock;

    public JsonPrintWriter(OutputStream out, boolean autoFlush) {
        this(out, autoFlush, Clock.systemUTC());
    }

    @SuppressForbidden(reason = "Override PrintWriter to emit Json")
    protected JsonPrintWriter(OutputStream out, boolean autoFlush, Clock clock) {
        super(out, autoFlush);
        this.clock = clock;
    }

    private void initJsonIfEmpty() {
        assert Thread.holdsLock(buffer); // only allow if holding lock on buffer
        if (buffer.isEmpty()) {
            buffer.append("{\"@timestamp\":\"");
            ISO_INSTANT_FORMATTER.formatTo(clock.instant(), buffer);
            buffer.append("\", \"message\":\"");
        }
    }

    @Override
    public void write(int c) {
        synchronized (buffer) {
            initJsonIfEmpty();
            JsonUtils.quote((char) c, buffer);
        }
    }

    @Override
    public void write(char[] chars, int off, int len) {
        synchronized (buffer) {
            initJsonIfEmpty();
            JsonUtils.quoteAsString(chars, off, len, buffer);
        }
    }

    @Override
    public void write(String msg, int off, int len) {
        synchronized (buffer) {
            initJsonIfEmpty();
            JsonUtils.quoteAsString(msg, off, len, buffer);
        }
    }

    @Override
    public void println(String msg) {
        synchronized (lock) {
            if (isJsonObject(msg)) {
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
            if (isJsonObject(chars)) {
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

    @Override
    public void println(Object x) {
        println(String.valueOf(x));
    }

    @Override
    public void println() {
        synchronized (lock) {
            writeBufferedJson();
            super.println();
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
        synchronized (buffer) {
            if (buffer.length() > 0) {
                json = buffer.append("\"}").toString();
                if (buffer.capacity() > BUFFER_TRIM_THRESHOLD) {
                    buffer.setLength(buffer.capacity() >> 1); // reduce capacity again by half
                    buffer.trimToSize();
                }
                buffer.setLength(0); // clear buffer
            }
        }
        if (json != null) super.write(json, 0, json.length());
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
