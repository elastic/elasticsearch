/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

/**
 * Represents a structured message parsed from raw text.
 * <p>
 * A patterned message contains the original text's structure as a pattern,
 * an extracted timestamp, and an array of typed arguments.
 *
 * @param pattern   the pattern of the message, with placeholders for arguments
 * @param timestamp the timestamp extracted from the message
 * @param arguments an array of typed arguments extracted from the message
 */
public record PatternedMessage(String pattern, Timestamp timestamp, Argument<?>[] arguments) {

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PatternedMessage{");
        sb.append("pattern='").append(pattern).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", arguments=");
        if (arguments != null) {
            sb.append('[');
            for (int i = 0; i < arguments.length; i++) {
                Argument<?> argument = arguments[i];
                sb.append(argument.type().name()).append(':').append(argument.encode());
                if (i < arguments.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(']');
        }
        sb.append('}');
        return sb.toString();
    }
}
