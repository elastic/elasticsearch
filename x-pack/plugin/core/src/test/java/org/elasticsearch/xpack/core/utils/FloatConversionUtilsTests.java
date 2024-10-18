/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.utils;

import org.elasticsearch.test.ESTestCase;

public class FloatConversionUtilsTests extends ESTestCase {

    public void testFloatArrayOf() {
        double[] doublesArray = { 1.0, 2.0, 3.0 };
        float[] floatArray = FloatConversionUtils.floatArrayOf(doublesArray);
        assertEquals(1.0, floatArray[0], 0.0);
        assertEquals(2.0, floatArray[1], 0.0);
        assertEquals(3.0, floatArray[2], 0.0);
    }

}
