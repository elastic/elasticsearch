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

public class EndsWithProcessorTests extends ESTestCase {

    public void testStartsWithFunctionWithValidInput() {
        assertEquals(true, new EndsWith(EMPTY, l("foobarbar"), l("r")).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foobar"), l("foo")).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foo"), l("foobar")).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foobar"), l("")).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("foo")).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("oO")).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("FOo")).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l('f'), l('f')).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l(""), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
    }
    
    public void testStartsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l(5), l("foo")).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l("bar"), l(false)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testStartsWithFunctionWithRandomInvalidDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, literal, l("foo")).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l("foo"), literal).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
    }
}