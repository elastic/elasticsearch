/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

public class StringFunctionsTests extends ESTestCase {

    public void testLength() {
        assertEquals(Integer.valueOf(0), Length.process(""));
        assertEquals(Integer.valueOf(1), Length.process("a"));
        assertEquals(Integer.valueOf(2), Length.process("❗️"));
        assertEquals(Integer.valueOf(100), Length.process(randomUnicodeOfLength(100)));
        assertEquals(Integer.valueOf(100), Length.process(randomAlphaOfLength(100)));
        assertNull(Length.process(null));
    }

}
