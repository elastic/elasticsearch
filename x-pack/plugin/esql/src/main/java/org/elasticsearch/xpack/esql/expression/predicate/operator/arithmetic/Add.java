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
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.ADD;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAddExact;

public class Add extends DateTimeArithmeticOperation implements BinaryComparisonInversible {

    public Add(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            ADD,
            AddIntsEvaluator.Factory::new,
            AddLongsEvaluator.Factory::new,
            AddUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new AddDoublesEvaluator.Factory(lhs, rhs),
            AddDatetimesEvaluator.Factory::new
        );
    }

    @Override
    protected NodeInfo<Add> info() {
        return NodeInfo.create(this, Add::new, left(), right());
    }

    @Override
    protected Add replaceChildren(Expression left, Expression right) {
        return new Add(source(), left, right);
    }

    @Override
    public Add swapLeftAndRight() {
        return new Add(source(), right(), left());
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Sub::new;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.addExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.addExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    public static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongAddExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs + rhs;
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount) {
        // using a UTC conversion since `datetime` is always a UTC-Epoch timestamp, either read from ES or converted through a function
        return asMillis(asDateTime(datetime).plus(temporalAmount));
    }

    @Override
    public Period fold(Period left, Period right) {
        return left.plus(right);
    }

    @Override
    public Duration fold(Duration left, Duration right) {
        return left.plus(right);
    }
}
