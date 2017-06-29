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

public class Or extends BinaryExpression implements Negateable {

    public Or(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    public Or swapLeftAndRight() {
        return new Or(location(), right(), left());
    }

    @Override
    public And negate() {
        return new And(location(), new Not(location(), left()), new Not(location(), right()));
    }

    @Override
    public String symbol() {
        return "||";
    }
}
