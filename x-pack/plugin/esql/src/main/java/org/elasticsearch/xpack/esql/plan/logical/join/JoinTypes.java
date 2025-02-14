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
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /**
     * Join type for the USING clause - shorthand for defining an equi-join (equality join meaning the condition checks if columns across
     * each side of the join are equal).
     * One important difference is that the USING clause returns the join column only once, at the beginning of the result set.
     */
    public static class UsingJoinType implements JoinType {
        private final List<Attribute> columns;
        private final JoinType coreJoin;

        public UsingJoinType(JoinType coreJoin, List<Attribute> columns) {
            this.columns = columns;
            this.coreJoin = coreJoin;
        }

        @Override
        public String joinName() {
            return coreJoin.joinName() + " USING " + columns.toString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IllegalArgumentException("USING join type should not be serialized due to being rewritten");
        }

        public JoinType coreJoin() {
            return coreJoin;
        }

        public List<Attribute> columns() {
            return columns;
        }

        @Override
        public boolean resolved() {
            return Resolvables.resolved(columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columns, coreJoin);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UsingJoinType that = (UsingJoinType) o;
            return Objects.equals(columns, that.columns) && coreJoin == that.coreJoin;
        }

        @Override
        public String toString() {
            return joinName();
        }
    }

    /**
     * Private class so it doesn't get used yet it is defined to showcase why the join type was defined as an interface instead of a simpler
     * enum.
     */
    private abstract static class NaturalJoinType implements JoinType {

        private final JoinType joinType;

        private NaturalJoinType(JoinType joinType) {
            this.joinType = joinType;
        }

        @Override
        public String joinName() {
            return "NATURAL " + joinType.joinName();
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
