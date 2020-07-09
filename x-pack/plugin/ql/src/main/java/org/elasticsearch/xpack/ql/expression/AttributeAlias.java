/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression;

import java.util.Objects;

public class AttributeAlias {
    private final Attribute attribute;
    private final Expression expression;

    public AttributeAlias(Attribute attribute, Expression expression) {
        this.attribute = attribute;
        this.expression = expression;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AttributeAlias that = (AttributeAlias) o;
        return Objects.equals(attribute, that.attribute) &&
            Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attribute, expression);
    }

    @Override
    public String toString() {
        return "Alias {" + attribute.toString() + " -> " + expression.toString() + "}";
    }
}
