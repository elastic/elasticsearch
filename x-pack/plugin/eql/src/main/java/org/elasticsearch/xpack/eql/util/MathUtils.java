/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

public class MathUtils {

    public static int abs(int number) {

        if (number == Integer.MIN_VALUE) {
            // TODO: can this function be removed?
            // This case should never occur, as `number` is either a non-negative user-provided input,
            // or the result of opposing sign integers summation.
            // Additionally, the math on offset/limit is inexact anyways.
            // But, if this can somehow happen, we should (1) have a test and (2) switch to exact math everywhere.
            throw new EqlIllegalArgumentException("[" + number + "] cannot be negated since the result is outside the range");
        }

        return number < 0 ? -number : number;
    }
}
