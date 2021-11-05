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

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.startsWith;

public class EndsWithFunctionProcessorTests extends ESTestCase {

    public void testEndsWithFunctionWithValidInputCaseSensitive() {
        assertTrue(sensitiveEndsWith("foobarbar", "r"));
        assertFalse(sensitiveEndsWith("foobarbar", "R"));
        assertTrue(sensitiveEndsWith("foobarbar", "bar"));
        assertFalse(sensitiveEndsWith("foobarBar", "bar"));
        assertFalse(sensitiveEndsWith("foobar", "foo"));
        assertFalse(sensitiveEndsWith("foo", "foobar"));
        assertTrue(sensitiveEndsWith("foobar", ""));
        assertTrue(sensitiveEndsWith("foo", "foo"));
        assertFalse(sensitiveEndsWith("foo", "oO"));
        assertFalse(sensitiveEndsWith("foo", "FOo"));
        assertFalse(sensitiveEndsWith("", "bar"));
        assertNull(sensitiveEndsWith(null, "bar"));
        assertNull(sensitiveEndsWith("foo", null));
        assertNull(sensitiveEndsWith(null, null));

        assertEquals(true, new EndsWith(EMPTY, l('f'), l('f'), true).makePipe().asProcessor().process(null));
    }

    private Boolean sensitiveEndsWith(String left, String right) {
        return endsWith(false, left, right);
    }

    public void testEndsWithFunctionWithValidInputCaseInsensitive() {
        assertTrue(insensitiveEndsWith("foobarbar", "r"));
        assertTrue(insensitiveEndsWith("foobarbar", "R"));
        assertFalse(insensitiveEndsWith("foobar", "foo"));
        assertFalse(insensitiveEndsWith("foo", "foobar"));
        assertTrue(insensitiveEndsWith("foobar", ""));
        assertTrue(insensitiveEndsWith("foo", "foo"));
        assertTrue(insensitiveEndsWith("foo", "oO"));
        assertTrue(insensitiveEndsWith("foo", "FOo"));
        assertFalse(insensitiveEndsWith("", "bar"));
        assertNull(insensitiveEndsWith(null, "bar"));
        assertNull(insensitiveEndsWith("foo", null));
        assertNull(insensitiveEndsWith(null, null));

        assertEquals(true, new EndsWith(EMPTY, l('f'), l('f'), true).makePipe().asProcessor().process(null));
    }

    private Boolean insensitiveEndsWith(String left, String right) {
        return endsWith(true, left, right);
    }

    private Boolean endsWith(boolean caseInsensitive, String left, String right) {
        return (Boolean) new EndsWith(EMPTY, l(left), l(right), caseInsensitive).makePipe().asProcessor().process(null);
    }

    private Boolean untypedEndsWith(Object left, Object right) {
        return (Boolean) new EndsWith(EMPTY, l(left), l(right), randomBoolean()).makePipe().asProcessor().process(null);
    }

    public void testEndsWithFunctionInputsValidation() {
        boolean caseSensitive = randomBoolean();
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> untypedEndsWith(5, "foo"));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class, () -> untypedEndsWith("bar", false));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testEndsWithFunctionWithRandomInvalidDataType() {
        boolean caseSensitive = randomBoolean();
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> untypedEndsWith(literal, "foo"));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class, () -> untypedEndsWith("foo", literal));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
    }
}
