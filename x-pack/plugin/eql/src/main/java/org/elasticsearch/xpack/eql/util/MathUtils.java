/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

public class MathUtils {

    public static int abs(int number) {

        if (number == Integer.MIN_VALUE) {
            throw new EqlIllegalArgumentException("[" + number + "] cannot be negated since the result is outside the range");
        }

        return number < 0 ? -number : number;
    }
}
