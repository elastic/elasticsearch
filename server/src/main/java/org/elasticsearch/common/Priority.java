/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public enum Priority {

    IMMEDIATE((byte) 0),
    URGENT((byte) 1),
    HIGH((byte) 2),
    NORMAL((byte) 3),
    LOW((byte) 4),
    LANGUID((byte) 5);

    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        output.writeByte(priority.value);
    }

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
