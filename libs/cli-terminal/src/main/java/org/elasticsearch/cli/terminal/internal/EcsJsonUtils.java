/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

/**
 * Minimal, zero-dependency ECS JSON formatter for CLI and launcher output.
 *
 * <p> Formats messages as single-line JSON conforming to a subset of the Elastic Common Schema.
 * This is intentionally self-contained (no log4j, no ecs-logging-core) so it can be used
 * in the GraalVM native-image launcher as well as the CLI preparer process.
 *
 * <p> The {@link #looksLikeJson} method can be used to check whether a line already
 * looks like JSON (starts with a left curly brace) and should be passed through unchanged.
 */
public final class EcsJsonUtils {

    static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder().appendInstant(3).toFormatter(Locale.ROOT);

    private EcsJsonUtils() {}

    public static boolean useJsonOutput() {
        return "true".equals(System.getenv("ELASTIC_CONTAINER"));
    }

    /**
     * Formats a plain message as a single-line ECS JSON string.
     */
    public static String formatJson(String level, String logger, CharSequence message) {
        StringBuilder sb = new StringBuilder(256);
        appendPreamble(sb, level, logger);
        sb.append("\"message\":\"");
        escapeJson(sb, message);
        sb.append("\"}");
        return sb.toString();
    }

    /**
     * Formats a message with a throwable as a single-line ECS JSON string (no trailing newline).
     * The full stacktrace goes into {@code error.stack_trace} and the exception class into {@code error.type}.
     */
    public static String formatJson(String level, String logger, CharSequence message, Throwable throwable) {
        StringBuilder sb = new StringBuilder(1024);
        appendPreamble(sb, level, logger);
        sb.append("\"message\":\"");
        escapeJson(sb, message != null ? message : throwable.toString());
        sb.append("\",\"error.type\":\"");
        escapeJson(sb, throwable.getClass().getName());
        sb.append("\",\"error.stack_trace\":\"");
        escapeJson(sb, stackTraceToString(throwable));
        sb.append("\"}");
        return sb.toString();
    }

    /**
     * Returns {@code true} if the line appears to already be a JSON object.
     */
    public static boolean looksLikeJson(CharSequence line) {
        return line.length() > 0 && line.charAt(0) == '{';
    }

    private static void appendPreamble(StringBuilder sb, String level, String logger) {
        sb.append("{\"@timestamp\":\"");
        sb.append(TIMESTAMP_FORMATTER.format(Instant.now()));
        sb.append("\",\"log.level\":\"");
        sb.append(level);
        sb.append("\",\"log.logger\":\"");
        escapeJson(sb, logger);
        sb.append("\",\"service.name\":\"ES_ECS\",\"ecs.version\":\"1.2.0\",");
    }

    private static String stackTraceToString(Throwable throwable) {
        StringWriter sw = new StringWriter(512);
        throwable.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    static void escapeJson(StringBuilder sb, CharSequence chars) {
        if (chars == null) {
            return;
        }
        for (int i = 0; i < chars.length(); i++) {
            char c = chars.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\f' -> sb.append("\\f");
                case '\r' -> sb.append("\\r");
                default -> {
                    if (c < 0x20) {
                        sb.append("\\u00");
                        sb.append(Character.forDigit((c >> 4) & 0xF, 16));
                        sb.append(Character.forDigit(c & 0xF, 16));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
    }
}
