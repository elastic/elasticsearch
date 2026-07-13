/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

public class NumberUtilsTests extends ESTestCase {
    public void testConvertNumberToString() {
        assertEquals("872.6182", NumberUtils.doubleToString(872.6181989583333d));
        assertEquals("1222.1833", NumberUtils.doubleToString(1222.18325d));
        assertEquals("1222.1832", NumberUtils.doubleToString(1222.18324d));
        assertEquals("1.0013", NumberUtils.doubleToString(1.0013d));
        assertEquals("10.0220", NumberUtils.doubleToString(10.022d));
        assertEquals("222.0000", NumberUtils.doubleToString(222.0d));
        assertEquals("0.0001", NumberUtils.doubleToString(0.0001d));
    }

    public void testConvertZeroToString() {
        assertEquals("0", NumberUtils.doubleToString(0.0d));
        assertEquals("0", NumberUtils.doubleToString(0.00009d));
    }
}
