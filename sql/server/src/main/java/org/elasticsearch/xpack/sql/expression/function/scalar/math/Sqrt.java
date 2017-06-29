/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

public class Sqrt extends MathFunction {

    public Sqrt(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    protected Double math(double d) {
        return Math.sqrt(d);
    }
}
