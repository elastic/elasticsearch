/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.startsWith;

public class StartsWithProcessorTests extends ESTestCase {

    public void testStartsWithFunctionWithValidInput() {
        assertEquals(true, new StartsWith(EMPTY, l("foobarbar"), l("f")).makePipe().asProcessor().process(null));
        assertEquals(false, new StartsWith(EMPTY, l("foobar"), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(false, new StartsWith(EMPTY, l("foo"), l("foobar")).makePipe().asProcessor().process(null));
        assertEquals(true, new StartsWith(EMPTY, l("foobar"), l("")).makePipe().asProcessor().process(null));
        assertEquals(true, new StartsWith(EMPTY, l("foo"), l("foo")).makePipe().asProcessor().process(null));
        assertEquals(true, new StartsWith(EMPTY, l("foo"), l("FO")).makePipe().asProcessor().process(null));
        assertEquals(true, new StartsWith(EMPTY, l("foo"), l("FOo")).makePipe().asProcessor().process(null));
        assertEquals(true, new StartsWith(EMPTY, l('f'), l('f')).makePipe().asProcessor().process(null));
        assertEquals(false, new StartsWith(EMPTY, l(""), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(null, new StartsWith(EMPTY, l(null), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(null, new StartsWith(EMPTY, l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(null, new StartsWith(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
    }
    
    public void testStartsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new StartsWith(EMPTY, l(5), l("foo")).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new StartsWith(EMPTY, l("bar"), l(false)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testStartsWithFunctionWithRandomInvalidDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new StartsWith(EMPTY, literal, l("foo")).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new StartsWith(EMPTY, l("foo"), literal).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
    }
}