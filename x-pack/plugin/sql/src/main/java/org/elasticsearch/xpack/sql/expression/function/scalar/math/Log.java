/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * <a href="https://en.wikipedia.org/wiki/Natural_logarithm">Natural logarithm</a>
 * function.
 */
public class Log extends MathFunction {
    public Log(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Log> info() {
        return NodeInfo.create(this, Log::new, field());
    }

    @Override
    protected Log replaceChild(Expression newChild) {
        return new Log(location(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.LOG;
    }
}
