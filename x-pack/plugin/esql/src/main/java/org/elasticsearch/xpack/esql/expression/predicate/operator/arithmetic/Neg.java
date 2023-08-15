/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;

public class Neg {
    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int v) {
        return Math.negateExact(v);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long v) {
        return Math.negateExact(v);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double v) {
        // This can never fail (including when `v` is +/- infinity or NaN) since negating a double is just a bit flip.
        return -v;
    }
}
