/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.BinaryExpression.Negateable;
import org.elasticsearch.xpack.sql.tree.Location;

public class GreaterThan extends BinaryComparison implements Negateable {

    public GreaterThan(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    public LessThan swapLeftAndRight() {
        return new LessThan(location(), right(), left());
    }

    @Override
    public LessThanOrEqual negate() {
        return new LessThanOrEqual(location(), left(), right());
    }

    @Override
    public String symbol() {
        return ">";
    }
}
