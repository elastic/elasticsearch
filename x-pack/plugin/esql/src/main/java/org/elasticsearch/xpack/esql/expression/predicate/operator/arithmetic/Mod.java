/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;

public class Mod {
    @Evaluator(extraName = "Ints")
    static int processInts(int lhs, int rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "Longs")
    static long processLongs(long lhs, long rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs % rhs;
    }
}
