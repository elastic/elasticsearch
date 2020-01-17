/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Subtraction function ({@code a - b}).
 */
public class Sub extends DateTimeArithmeticOperation {

    public Sub(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryArithmeticOperation.SUB);
    }

    @Override
    protected NodeInfo<Sub> info() {
        return NodeInfo.create(this, Sub::new, left(), right());
    }

    @Override
    protected Sub replaceChildren(Expression newLeft, Expression newRight) {
        return new Sub(source(), newLeft, newRight);
    }

    @Override
    protected TypeResolution resolveWithIntervals() {
        TypeResolution resolution = super.resolveWithIntervals();
        if (resolution.unresolved()) {
            return resolution;
        }
        if ((right().dataType().isDateOrTimeBased()) && left().dataType().isInterval()) {
            return new TypeResolution(format(null, "Cannot subtract a {}[{}] from an interval[{}]; do you mean the reverse?",
                right().dataType().typeName(), right().source().text(), left().source().text()));
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
