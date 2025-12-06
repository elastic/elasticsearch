/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Utility class defining the concrete types of joins supported by ESQL.
 */
public class JoinTypes {

    private JoinTypes() {}

    public static JoinType INNER = CoreJoinType.INNER;
    public static JoinType LEFT = CoreJoinType.LEFT;
    public static JoinType RIGHT = CoreJoinType.RIGHT;
    public static JoinType FULL = CoreJoinType.FULL;
    public static JoinType CROSS = CoreJoinType.CROSS;

    private static Map<Byte, JoinType> JOIN_TYPES;

    static {
        CoreJoinType[] types = CoreJoinType.values();
        JOIN_TYPES = Maps.newMapWithExpectedSize(types.length);
        for (CoreJoinType type : types) {
            JOIN_TYPES.put(type.id, type);
        }
    }

    /**
     * The predefined core join types. Implements as enum for easy comparison and serialization.
     */
    private enum CoreJoinType implements JoinType {
        INNER(1, "INNER"),
        LEFT(2, "LEFT OUTER"),
        RIGHT(3, "RIGHT OUTER"),
        FULL(4, "FULL OUTER"),
        CROSS(5, "CROSS");

        private final String name;
        private final byte id;

        CoreJoinType(int id, String name) {
            this.id = (byte) id;
            this.name = name;
        }

        @Override
        public String joinName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }

    public static JoinType readFrom(StreamInput in) throws IOException {
        byte id = in.readByte();
        JoinType type = JOIN_TYPES.get(id);
        if (type == null) {
            throw new IllegalArgumentException("unsupported join [" + id + "]");
        }
        ;
        return type;
    }
}
