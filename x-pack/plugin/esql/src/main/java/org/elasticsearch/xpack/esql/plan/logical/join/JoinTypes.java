/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.Objects;

/**
 * Utility class defining the concrete types of joins supported by ESQL.
 */
public class JoinTypes {

    private JoinTypes() {}

    public static CoreJoinType INNER = CoreJoinType.INNER;
    public static CoreJoinType LEFT = CoreJoinType.LEFT;
    public static CoreJoinType RIGHT = CoreJoinType.RIGHT;
    public static CoreJoinType FULL = CoreJoinType.FULL;
    public static CoreJoinType CROSS = CoreJoinType.CROSS;

    /**
     * The predefined core join types. Implements as enum for easy comparison and serialization.
     */
    public enum CoreJoinType implements JoinType {
        INNER("INNER"),
        LEFT("LEFT OUTER"),
        RIGHT("RIGHT OUTER"),
        FULL("FULL OUTER"),
        CROSS("CROSS");

        private final String name;

        CoreJoinType(String name) {
            this.name = name;
        }

        @Override
        public String joinName() {
            return name;
        }
    }

    /**
     * Join type for the USING clause - shorthand for defining an equi-join (equality join meaning the condition checks if columns across
     * each side of the join are equal).
     * One important difference is that the USING clause does not include the join columns in the result set.
     */
    public static class UsingJoinType implements JoinType {
        private final List<Attribute> columns;
        private final CoreJoinType coreJoin;

        public UsingJoinType(CoreJoinType coreJoin, List<Attribute> columns) {
            this.columns = columns;
            this.coreJoin = coreJoin;
        }

        @Override
        public String joinName() {
            return "USING " + columns.toString();
        }

        public CoreJoinType coreJoin() {
            return coreJoin;
        }

        public List<Attribute> columns() {
            return columns;
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
}
