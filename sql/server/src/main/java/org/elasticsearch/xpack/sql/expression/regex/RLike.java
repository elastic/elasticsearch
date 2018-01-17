/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.regex;

import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.regex.Pattern;

public class RLike extends BinaryExpression {

    public RLike(Location location, Expression left, Literal right) {
        super(location, left, right);
    }

    @Override
    public Literal right() {
        return (Literal) super.right();
    }

    @Override
    public Object fold() {
        Pattern p = Pattern.compile(right().fold().toString());
        return p.matcher(left().fold().toString()).matches();
    }

    @Override
    public RLike swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public String symbol() {
        return "RLIKE";
    }
}
