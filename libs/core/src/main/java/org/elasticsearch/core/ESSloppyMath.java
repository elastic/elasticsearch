/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * Similar to Lucene's SloppyMath, but for additional math functions.
 */
public class ESSloppyMath {

    private ESSloppyMath() {}

    public static double sinh(double value) {
        return FastMath.sinh(value);
    }

    public static double atan(double value) {
        return FastMath.atan(value);
    }

    public static double log(double value) {
        return FastMath.log(value);
    }
}
