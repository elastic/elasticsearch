/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

public class NumberUtilsTests extends ESTestCase {
    public void testConvertToString() {
        assertEquals("872.6181", NumberUtils.doubleToString(872.6181989583333d));
        assertEquals("1.0013", NumberUtils.doubleToString(1.0013d));
        assertEquals("10.0220", NumberUtils.doubleToString(10.022d));
        assertEquals("222.0000", NumberUtils.doubleToString(222.0d));
    }
}
