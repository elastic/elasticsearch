/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.UnaryExpression;
import org.elasticsearch.xpack.sql.expression.BinaryExpression.Negateable;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class Not extends UnaryExpression {

    public Not(Location location, Expression child) {
        super(location, child);
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
        return DataTypes.BOOLEAN;
    }
}
