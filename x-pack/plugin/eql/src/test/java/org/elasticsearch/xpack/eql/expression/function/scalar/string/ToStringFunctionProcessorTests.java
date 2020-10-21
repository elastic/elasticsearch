/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class ToStringFunctionProcessorTests extends ESTestCase {

    public void testToStringFunctionWithValidInput() {
        assertToStringWithObject("foo");
        assertToStringWithObject(123);
        assertToStringWithObject(1234L);
        assertToStringWithObject(123.456);
        assertToStringWithObject(123.456f);
        assertToStringWithObject(123.456d);
        assertToStringWithObject('f');
        assertToStringWithObject(null);
    }

    private void assertToStringWithObject(Object input) {
        Object str = new ToString(EMPTY, l(input)).makePipe().asProcessor().process(null);
        assertEquals(input == null ? null : input.toString(), str);
        if (str != null) {
            assertTrue(str instanceof String);
        }
    }
}
