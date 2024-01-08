/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import co.elastic.logging.EcsJsonSerializer;

import org.elasticsearch.core.SuppressForbidden;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Clock;

/**
 * {@link PrintWriter} formatting lines as JSON using a subset of fields written by {@link EcsJsonSerializer}.
 * Anything that looks like a JSON object is immediately passed through and written as is.
 */
public class JsonPrintWriter extends PrintWriter {
    private static final int INITIAL_BUFFER_SIZE = 256;
    private static final int MAX_BUFFER_SIZE = 4 * INITIAL_BUFFER_SIZE;

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
            json = toJson(buffer.isEmpty() ? msg : getAndClearBuffer(msg), clock.millis());
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
                super.write(toJson(getAndClearBuffer(), clock.millis()));
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
     * Converts {@code msg} to JSON using the {@link EcsJsonSerializer}.
     */
    protected static String toJson(String msg, long timeMillis) {
        StringBuilder builder = new StringBuilder(INITIAL_BUFFER_SIZE);
        EcsJsonSerializer.serializeObjectStart(builder, timeMillis);
        EcsJsonSerializer.serializeFormattedMessage(builder, msg);
        stripTrailing(builder, ' ');
        stripTrailing(builder, ',');
        EcsJsonSerializer.serializeObjectEnd(builder);
        stripTrailing(builder, '\n');
        return builder.toString();
    }

    private static void stripTrailing(StringBuilder builder, char c) {
        if (builder.length() > 0 && builder.charAt(builder.length() - 1) == c) {
            builder.setLength(builder.length() - 1);
        }
    }

    /**
     * Checks if {@code str} is likely a JSON object surrounded by `{` and `}` without any further validation.
     * Leading and trailing whitespaces are ignored.
     */
    protected static boolean isJsonObject(String str) {
        if (str == null) return false;

        int i = 0;
        while (i < str.length() && Character.isWhitespace(str.charAt(i))) {
            i++;
        }
        if (i == str.length() || str.charAt(i) != '{') return false;

        i = str.length() - 1;
        while (i > 0 && Character.isWhitespace(str.charAt(i))) {
            i--;
        }
        return str.charAt(i) == '}';
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
