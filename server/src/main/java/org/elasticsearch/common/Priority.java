/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents the priority levels for tasks and operations in Elasticsearch.
 * Priority determines the order in which tasks are executed, with higher priority
 * tasks running before lower priority ones.
 *
 * <p>The priority levels in order from highest to lowest are:
 * {@link #IMMEDIATE}, {@link #URGENT}, {@link #HIGH}, {@link #NORMAL}, {@link #LOW}, {@link #LANGUID}.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Set task priority
 * Priority priority = Priority.HIGH;
 *
 * // Compare priorities
 * if (priority.after(Priority.NORMAL)) {
 *     // This priority runs after NORMAL priority
 * }
 *
 * // Serialize/deserialize
 * StreamOutput out = ...;
 * Priority.writeTo(Priority.URGENT, out);
 *
 * StreamInput in = ...;
 * Priority p = Priority.readFrom(in);
 * }</pre>
 */
public enum Priority {

    IMMEDIATE((byte) 0),
    URGENT((byte) 1),
    HIGH((byte) 2),
    NORMAL((byte) 3),
    LOW((byte) 4),
    LANGUID((byte) 5);

    /**
     * Reads a Priority value from the stream input.
     *
     * @param input the stream to read from
     * @return the Priority read from the stream
     * @throws IOException if an I/O error occurs
     */
    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    /**
     * Writes a Priority value to the stream output.
     *
     * @param priority the priority to write
     * @param output the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        output.writeByte(priority.value);
    }

    /**
     * Converts a byte value to its corresponding Priority.
     *
     * @param b the byte value (0-5)
     * @return the Priority corresponding to the byte value
     * @throws IllegalArgumentException if the byte value does not correspond to a valid Priority
     */
    public static Priority fromByte(byte b) {
        return switch (b) {
            case 0 -> IMMEDIATE;
            case 1 -> URGENT;
            case 2 -> HIGH;
            case 3 -> NORMAL;
            case 4 -> LOW;
            case 5 -> LANGUID;
            default -> throw new IllegalArgumentException("can't find priority for [" + b + "]");
        };
    }

    private final byte value;

    Priority(byte value) {
        this.value = value;
    }

    /**
     * @return whether tasks of {@code this} priority will run after those of priority {@code p}.
     *         For instance, {@code Priority.URGENT.after(Priority.IMMEDIATE)} returns {@code true}.
     */
    public boolean after(Priority p) {
        return this.compareTo(p) > 0;
    }

    /**
     * @return whether tasks of {@code this} priority will run no earlier than those of priority {@code p}.
     *         For instance, {@code Priority.URGENT.sameOrAfter(Priority.IMMEDIATE)} returns {@code true}.
     */
    public boolean sameOrAfter(Priority p) {
        return this.compareTo(p) >= 0;
    }

}
