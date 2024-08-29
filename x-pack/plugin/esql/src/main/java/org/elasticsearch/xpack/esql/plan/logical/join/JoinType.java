/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public enum JoinType implements Writeable {
    INNER(0, "INNER"),
    LEFT(1, "LEFT OUTER"),
    RIGHT(2, "RIGHT OUTER"),
    FULL(3, "FULL OUTER"),
    CROSS(4, "CROSS");

    private final byte id;
    private final String name;

    JoinType(int id, String name) {
        this.id = (byte) id;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }

    public static JoinType readFrom(StreamInput in) throws IOException {
        byte id = in.readByte();
        return switch (id) {
            case 0 -> INNER;
            case 1 -> LEFT;
            case 2 -> RIGHT;
            case 3 -> FULL;
            case 4 -> CROSS;
            default -> throw new IllegalArgumentException("unsupported join [" + id + "]");
        };
    }
}
