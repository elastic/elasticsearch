/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.common.Strings;

import java.util.Objects;

public class EqlFoldSpec {

    private final String name;
    private final String description;
    private final String expression;
    private final Object expected;

    EqlFoldSpec(String name, String description, String expression, Object expected) {
        this.name = name;
        this.description = description;
        this.expression = expression;
        this.expected = expected;
    }

    public String expression() {
        return expression;
    }

    public Object expected() {
        return expected;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendWithComma(sb, "name", name);
        appendWithComma(sb, "expression", expression);
        appendWithComma(sb, "expected", expected == null ? "null" : expected);
        return sb.toString();
    }

    private static void appendWithComma(StringBuilder builder, String key, Object value) {
        if (value != null) {
            String valueStr = value.toString();

            if (Strings.isEmpty(valueStr) == false) {
                if (builder.length() > 0) {
                    builder.append(", ");
                }
                builder.append(key);
                builder.append(": ");
                builder.append(valueStr);
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EqlFoldSpec that = (EqlFoldSpec) other;

        return Objects.equals(this.expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.expression);
    }
}
