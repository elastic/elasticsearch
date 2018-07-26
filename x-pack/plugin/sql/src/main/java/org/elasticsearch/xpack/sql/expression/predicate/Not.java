/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.BinaryOperator.Negateable;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.UnaryExpression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class Not extends UnaryExpression {

    public Not(Location location, Expression child) {
        super(location, child);
    }

    @Override
    protected NodeInfo<Not> info() {
        return NodeInfo.create(this, Not::new, child());
    }

    @Override
    protected Not replaceChild(Expression newChild) {
        return new Not(location(), newChild);
    }

    protected TypeResolution resolveType() {
        if (DataType.BOOLEAN == child().dataType()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution("Cannot negate expression ([" + Expressions.name(child()) + "] of type ["
                + child().dataType().esType + "])");
    }

    @Override
    public Object fold() {
        return Objects.equals(child().fold(), Boolean.TRUE) ? Boolean.FALSE : Boolean.TRUE;
    }

    @Override
    protected Expression canonicalize() {
        Expression canonicalChild = child().canonical();
        if (canonicalChild instanceof Negateable) {
            return ((Negateable) canonicalChild).negate();
        }
        return this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }
}
