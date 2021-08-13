/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Subtraction function ({@code a - b}).
 */
public class Sub extends DateTimeArithmeticOperation implements BinaryComparisonInversible {

    public Sub(Source source, Expression left, Expression right) {
        super(source, left, right, SqlBinaryArithmeticOperation.SUB);
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
        if ((SqlDataTypes.isDateOrTimeBased(right().dataType())) && SqlDataTypes.isInterval(left().dataType())) {
            return new TypeResolution(format(null, "Cannot subtract a {}[{}] from an interval[{}]; do you mean the reverse?",
                right().dataType().typeName(), right().source().text(), left().source().text()));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Add::new;
    }
}
