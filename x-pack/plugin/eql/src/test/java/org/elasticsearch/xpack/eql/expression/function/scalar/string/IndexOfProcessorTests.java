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

public class IndexOfProcessorTests extends ESTestCase {

    public void testStartsWithFunctionWithValidInput() {
        assertEquals(5, new IndexOf(EMPTY, l("foobarbar"), l("r"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foobar"), l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l("foobar"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foo"), l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(1, new IndexOf(EMPTY, l("foo"), l("oO"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foo"), l("FOo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l('f'), l('f'), l(null)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(""), l("bar"), l(1)).makePipe().asProcessor().process(null));
        assertEquals(5, new IndexOf(EMPTY, l("foobarbar"), l("R"), l(5)).makePipe().asProcessor().process(null));
        assertEquals(2, new IndexOf(EMPTY, l("foobar"), l("O"), l(2)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foobar"), l("O"), l(3)).makePipe().asProcessor().process(null));
        assertEquals(6, new IndexOf(EMPTY, l("foobarbaz"), l("ba"), l(4)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l(null), l(4)).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("bar"), l("bar"), l(null)).makePipe().asProcessor().process(null));
    }
    
    public void testStartsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l(5), l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("bar"), l(false), l(2)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("bar"), l("a"), l("1")).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [1]", siae.getMessage());
    }

    public void testStartsWithFunctionWithRandomInvalidDataType() {
        Literal stringLiteral = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, stringLiteral, l("foo"), l(1)).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("foo"), stringLiteral, l(2)).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        
        Literal numericLiteral = randomValueOtherThanMany(v -> v.dataType().isNumeric(), () -> LiteralTests.randomLiteral());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("foo"), l("o"), numericLiteral).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A number is required; received"));
    }
}