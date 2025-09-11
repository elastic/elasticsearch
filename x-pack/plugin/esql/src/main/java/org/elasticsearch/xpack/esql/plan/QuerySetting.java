/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

public class QuerySetting {

    private final Source source;
    private final Alias field;

    public QuerySetting(Source source, Alias field) {
        this.source = source;
        this.field = field;
    }

    public Alias field() {
        return field;
    }

    public String name() {
        return field.name();
    }

    public Expression value() {
        return field.child();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QuerySetting eval = (QuerySetting) o;
        return Objects.equals(field, eval.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field);
    }

    public Source source() {
        return source;
    }

    @Override
    public String toString() {
        return "SET " + name() + " = " + value();
    }
}
