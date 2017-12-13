/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

public class Ceil extends MathFunction {
    public Ceil(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.CEIL;
    }

    @Override
    public DataType dataType() {
        return DataTypeConversion.asInteger(field().dataType());
    }
}
