/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class SubstringFunctionProcessorTests extends ESTestCase {

    public void testSubstringFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(
            QlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l(5), l("foo"), null).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(
            QlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("bar"), l(false), null).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [false]", siae.getMessage());
        siae = expectThrows(
            QlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("foo"), l(1), l("abc")).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [abc]", siae.getMessage());
    }
}
