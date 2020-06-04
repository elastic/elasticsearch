/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.common.Strings;

import java.util.Objects;

public class EqlFoldSpec {

    private final String name;
    private final String description;
    private final boolean caseSensitiveOnly;
    private final boolean caseInsensitiveOnly;
    private final String expression;
    private final Object expected;

    EqlFoldSpec(String name, String description, boolean caseSensitiveOnly,
                boolean caseInsensitiveOnly, String expression, Object expected) {
        this.name = name;
        this.description = description;
        this.caseInsensitiveOnly = caseInsensitiveOnly;
        this.caseSensitiveOnly = caseSensitiveOnly;
        this.expression = expression;
        this.expected = expected;
    }

    public String expression() {
        return expression;
    }

    public Object expected() {
        return expected;
    }

    public boolean supportsCaseSensitive() {
        return caseInsensitiveOnly == false;
    }

    public boolean supportsCaseInsensitive() {
        return caseSensitiveOnly == false;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendWithComma(sb, "name", name);
        appendWithComma(sb, "expression", expression);
        appendWithComma(sb, "case_sensitive", caseSensitiveOnly);
        appendWithComma(sb, "case_insensitive", caseInsensitiveOnly);
        appendWithComma(sb, "expected", expected == null ? "null": expected);
        return sb.toString();
    }

    private static void appendWithComma(StringBuilder builder, String key, Object value) {
        if (value != null) {
            String valueStr = value.toString();

            if (!Strings.isEmpty(valueStr)) {
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

        return Objects.equals(this.expression, that.expression)
            && Objects.equals(this.caseSensitiveOnly, that.caseSensitiveOnly)
            && Objects.equals(this.caseInsensitiveOnly, that.caseInsensitiveOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.expression, this.caseSensitiveOnly, this.caseInsensitiveOnly);
    }
}
