/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;
import org.elasticsearch.xpack.ql.session.Configuration;

import static org.elasticsearch.xpack.eql.EqlTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.startsWith;

public class IndexOfFunctionProcessorTests extends ESTestCase {

    public void testIndexOfFunctionWithValidInputInsensitive() {
        assertEquals(5, insensitiveIndexOf("foobarbar", "r", null));
        assertEquals(5, insensitiveIndexOf("foobaRbar", "r", null));
        assertEquals(0, insensitiveIndexOf("foobar", "Foo", null));
        assertNull(insensitiveIndexOf("foo", "foobar", null));
        assertEquals(0, insensitiveIndexOf("foo", "foo", null));
        assertEquals(1, insensitiveIndexOf("foo", "oO", null));
        assertEquals(0, insensitiveIndexOf("foo", "FOo", null));
        assertNull(insensitiveIndexOf("", "bar", 1));
        assertEquals(5, insensitiveIndexOf("foobarbar", "R", 5));
        assertEquals(2, insensitiveIndexOf("foobar", "O", 2));
        assertNull(insensitiveIndexOf("foobar", "O", 3));
        assertEquals(6, insensitiveIndexOf("foobarbaz", "ba", 4));
        assertNull(insensitiveIndexOf(null, "bar", 2));
        assertNull(insensitiveIndexOf(null, "bar", 2));
        assertNull(insensitiveIndexOf("foo", null, 3));
        assertNull(insensitiveIndexOf(null, null, 4));
        assertEquals(0, insensitiveIndexOf("bar", "bar", null));

        assertEquals(0, new IndexOf(EMPTY, l('f'), l('f'), null, false).makePipe().asProcessor().process(null));
    }

    private Object insensitiveIndexOf(String left, String right, Integer optional) {
        return indexOf(true, left, right, optional);
    }

    public void testIndexOfFunctionWithValidInputSensitive() {
        assertEquals(5, sensitiveIndexOf("foobarbar", "r", null));
        assertEquals(8, sensitiveIndexOf("foobaRbar", "r", null));
        assertEquals(4, sensitiveIndexOf("foobARbar", "AR", null));
        assertEquals(0, sensitiveIndexOf("foobar", "foo", null));
        assertNull(sensitiveIndexOf("foo", "foobar", null));
        assertEquals(0, sensitiveIndexOf("foo", "foo", null));
        assertNull(sensitiveIndexOf("foo", "oO", null));
        assertNull(sensitiveIndexOf("foo", "FOo", null));
        assertNull(sensitiveIndexOf("", "bar", 1));
        assertNull(sensitiveIndexOf("foobarbar", "R", 5));
        assertNull(sensitiveIndexOf("foobar", "O", 2));
        assertNull(sensitiveIndexOf("foobar", "O", 3));
        assertEquals(6, sensitiveIndexOf("foobarbaz", "ba", 4));
        assertNull(sensitiveIndexOf(null, "bar", 2));
        assertNull(sensitiveIndexOf(null, "bar", 2));
        assertNull(sensitiveIndexOf("foo", null, 3));
        assertNull(sensitiveIndexOf(null, null, 4));
        assertEquals(0, sensitiveIndexOf("bar", "bar", null));

        assertEquals(0, new IndexOf(EMPTY, l('f'), l('f'), null, true).makePipe().asProcessor().process(null));
    }

    private Object sensitiveIndexOf(String left, String right, Integer optional) {
        return indexOf(false, left, right, optional);
    }

    protected Object indexOf(boolean caseInsensitive, String left, String right, Integer optional) {
        return new IndexOf(EMPTY, l(left), l(right), l(optional), caseInsensitive).makePipe().asProcessor().process(null);
    }

    protected Object indexOfUntyped(Object left, Object right, Object optional) {
        return new IndexOf(EMPTY, l(left), l(right), l(optional), randomBoolean()).makePipe().asProcessor().process(null);
    }

    public void testIndexOfFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped(5, "foo", null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped("bar", false, 2));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped("bar", "a", "1"));
        assertEquals("A number is required; received [1]", siae.getMessage());
    }

    public void testIndexOfFunctionWithRandomInvalidDataType() {
        Configuration config = randomConfiguration();
        Literal stringLiteral = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped(stringLiteral, "foo", 1));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));

        siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped("foo", stringLiteral, 2));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));

        Literal numericLiteral = randomValueOtherThanMany(v -> v.dataType().isNumeric(), () -> LiteralTests.randomLiteral());
        siae = expectThrows(QlIllegalArgumentException.class, () -> indexOfUntyped("foo", "o", numericLiteral));
        assertThat(siae.getMessage(), startsWith("A number is required; received"));
    }
}
