/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.regex;

import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class Like extends BinaryExpression {

    public Like(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    public Like swapLeftAndRight() {
        return new Like(location(), right(), left());
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public String symbol() {
        return "LIKE";
    }
}
