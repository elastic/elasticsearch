/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public abstract class LeafInput<T> extends ProcessorDefinition {

    private T context;

    public LeafInput(Location location, Expression expression, T context) {
        super(location, expression, emptyList());
        this.context = context;
    }

    @Override
    public final ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
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

        LeafInput<?> other = (LeafInput<?>) obj;
        return Objects.equals(context(), other.context()) 
                && Objects.equals(expression(), other.expression());
    }
}
