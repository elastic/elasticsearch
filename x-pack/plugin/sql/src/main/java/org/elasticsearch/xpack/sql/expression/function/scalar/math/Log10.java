/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

/**
 * <a href="https://en.wikipedia.org/wiki/Logarithm">Logarithm</a>
 * base 10 function.
 */
public class Log10 extends MathFunction {
    public Log10(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Log10> info() {
        return NodeInfo.create(this, Log10::new, field());
    }

    @Override
    protected Log10 replaceChild(Expression newChild) {
        return new Log10(source(), newChild);
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.LOG10;
    }
}
