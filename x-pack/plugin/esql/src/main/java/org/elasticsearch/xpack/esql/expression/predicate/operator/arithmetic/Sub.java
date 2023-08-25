/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.DateTimeException;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.SUB;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongSubtractExact;

public class Sub extends DateTimeArithmeticOperation implements BinaryComparisonInversible {

    public Sub(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            SUB,
            SubIntsEvaluator::new,
            SubLongsEvaluator::new,
            SubUnsignedLongsEvaluator::new,
            (s, l, r) -> new SubDoublesEvaluator(l, r),
            SubDatetimesEvaluator::new
        );
    }

    @Override
    protected NodeInfo<Sub> info() {
        return NodeInfo.create(this, Sub::new, left(), right());
    }

    @Override
    protected Sub replaceChildren(Expression left, Expression right) {
        return new Sub(source(), left, right);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Add::new;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.subtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.subtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongSubtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs - rhs;
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount) {
        return asMillis(asDateTime(datetime, DEFAULT_TZ).minus(temporalAmount));
    }
}
