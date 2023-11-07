/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MOD;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;

public class Mod extends EsqlArithmeticOperation {

    public Mod(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            MOD,
            ModIntsEvaluator.Factory::new,
            ModLongsEvaluator.Factory::new,
            ModUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new ModDoublesEvaluator.Factory(lhs, rhs)
        );
    }

    @Override
    protected NodeInfo<Mod> info() {
        return NodeInfo.create(this, Mod::new, left(), right());
    }

    @Override
    protected Mod replaceChildren(Expression left, Expression right) {
        return new Mod(source(), left, right);
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return asLongUnsigned(Long.remainderUnsigned(asLongUnsigned(lhs), asLongUnsigned(rhs)));
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs % rhs;
    }
}
