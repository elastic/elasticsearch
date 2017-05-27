/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

public class Expm1 extends MathFunction {

    public Expm1(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    protected Double math(double d) {
        return Math.expm1(d);
    }
}
