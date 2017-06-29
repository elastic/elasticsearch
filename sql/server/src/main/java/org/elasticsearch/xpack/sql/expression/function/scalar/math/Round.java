/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class Round extends MathFunction {

    public Round(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    protected Long math(double d) {
        return Long.valueOf(Math.round(d));
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }
}
