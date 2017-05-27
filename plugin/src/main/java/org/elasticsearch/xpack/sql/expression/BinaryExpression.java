/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Arrays;
import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public abstract class BinaryExpression extends Expression {

    private final Expression left, right;

    public interface Negateable {
        BinaryExpression negate();
    }

    protected BinaryExpression(Location location, Expression left, Expression right) {
        super(location, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public boolean nullable() {
        return left.nullable() || left.nullable();
    }

    public abstract BinaryExpression swapLeftAndRight();

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        
        BinaryExpression other = (BinaryExpression) obj;
        return Objects.equals(left, other.left)
                && Objects.equals(right, other.right);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(left());
        sb.append(" ");
        sb.append(symbol());
        sb.append(" ");
        sb.append(right());
        return sb.toString();
    }

    // simplify toString
    public abstract String symbol();
}