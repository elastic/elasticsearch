/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.BinaryExpression.Negateable;
import org.elasticsearch.xpack.sql.tree.Location;

public class And extends BinaryExpression implements Negateable {
    
    public And(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    public Or negate() {
        return new Or(location(), new Not(location(), left()), new Not(location(), right()));
    }

    @Override
    public And swapLeftAndRight() {
        return new And(location(), right(), left());
    }

    @Override
    public String symbol() {
        return "&&";
    }
}
