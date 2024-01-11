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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

/**
 * {@link PrintWriter} formatting lines as JSON using a subset of fields written by ECSJsonLayout.
 * Anything that looks like a JSON object is immediately passed through and written as is.
 */
public class JsonPrintWriter extends PrintWriter {
    private static final int INITIAL_BUFFER_SIZE = 256;
    private static final int MAX_BUFFER_SIZE = 4 * INITIAL_BUFFER_SIZE;
    private static final DateTimeFormatter ISO_INSTANT_FORMATTER = new DateTimeFormatterBuilder().appendInstant(3).toFormatter(Locale.ROOT);

    private final StringBuilder buffer = new StringBuilder(INITIAL_BUFFER_SIZE);
    private final Clock clock;

    public JsonPrintWriter(OutputStream out, boolean autoFlush) {
        this(out, autoFlush, Clock.systemUTC());
    }

    @SuppressForbidden(reason = "Override PrintWriter to emit Json")
    protected JsonPrintWriter(OutputStream out, boolean autoFlush, Clock clock) {
        super(out, autoFlush);
        this.clock = clock;
    }

    @Override
    public void write(String msg) {
        if (isJsonObject(msg)) {
            super.write(msg);
        } else {
            synchronized (buffer) {
                buffer.append(msg);
            }
        }
    }

    @Override
    public void println(String msg) {
        final String json;
        if (isJsonObject(msg)) {
            // Forward as is, ignore current buffer.
            json = msg;
        } else {
            // Don't mind if another thread might miss a none-empty buffer.
            // It's highly unlikely this is meant to be concatenated into the same line.
            json = toJson(buffer.isEmpty() ? msg : getAndClearBuffer(msg), clock.instant());
        }
        super.write(json);
        super.println();
    }

    @Override
    public void println() {
        flush();
        super.println();
    }

    @Override
    public void flush() {
        synchronized (buffer) {
            if (buffer.isEmpty() == false) {
                super.write(toJson(getAndClearBuffer(), clock.instant()));
            }
        }
        super.flush();
    }

    @Override
    public void close() {
        flush();
        super.close();
    }

    /**
     * Converts {@code msg} to JSON mimicking the ECSJsonLayout.
     * Visible for testing.
     */
    protected static String toJson(String msg, Instant instant) {
        StringBuilder builder = new StringBuilder(INITIAL_BUFFER_SIZE);
        builder.append("{\"@timestamp\":\"");
        ISO_INSTANT_FORMATTER.formatTo(instant, builder);
        builder.append("\", \"message\":\"");
        JsonUtils.quoteAsString(msg, builder);
        builder.append("\"}");
        return builder.toString();
    }

    /**
     * Checks if {@code str} is likely a JSON object starting with `{` without any further validation.
     */
    protected static boolean isJsonObject(String str) {
        return str != null && str.length() > 0 && str.charAt(0) == '{';
    }

    private String getAndClearBuffer(String... parts) {
        synchronized (buffer) {
            for (String part : parts) {
                buffer.append(part);
            }
            String line = buffer.toString();
            if (buffer.capacity() > MAX_BUFFER_SIZE) {
                buffer.setLength(MAX_BUFFER_SIZE);
                buffer.trimToSize();
            }
            buffer.setLength(0); // clear buffer
            return line;
        }
    }
}
