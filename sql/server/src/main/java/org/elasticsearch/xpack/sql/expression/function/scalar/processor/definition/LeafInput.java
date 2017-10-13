/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;

import java.util.Objects;

import static java.util.Collections.emptyList;

public abstract class LeafInput<T> extends ProcessorDefinition {

    private T context;

    public LeafInput(Expression expression, T context) {
        super(expression, emptyList());
        this.context = context;
    }

    public T context() {
        return context;
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression(), context);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AggPathInput other = (AggPathInput) obj;
        return Objects.equals(context(), other.context()) 
                && Objects.equals(expression(), other.expression());
    }
}
