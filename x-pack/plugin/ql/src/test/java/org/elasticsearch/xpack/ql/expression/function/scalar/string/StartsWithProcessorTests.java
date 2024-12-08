/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;
import org.hamcrest.Matchers;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class StartsWithProcessorTests extends ESTestCase {

    public void testSensitiveStartsWithFunctionWithValidInput() {
        assertTrue(sensitiveStartsWith("foobarbar", "f"));
        assertFalse(sensitiveStartsWith("foobar", "bar"));
        assertFalse(sensitiveStartsWith("foo", "foobar"));
        assertTrue(sensitiveStartsWith("foobar", ""));
        assertTrue(sensitiveStartsWith("foo", "foo"));
        assertTrue(sensitiveStartsWith("FOoBar", "FOo"));
        assertFalse(sensitiveStartsWith("", "bar"));
        assertNull(sensitiveStartsWith(null, "bar"));
        assertNull(sensitiveStartsWith("foo", null));
        assertNull(sensitiveStartsWith(null, null));

        assertFalse(sensitiveStartsWith("foo", "FO"));
        assertFalse(sensitiveStartsWith("foo", "FOo"));
    }

    private Boolean sensitiveStartsWith(String left, String right) {
        return startsWith(false, left, right);
    }

    public void testInsensitiveStartsWithFunctionWithValidInput() {
        assertTrue(insensitiveStartsWith("foobarbar", "f"));
        assertFalse(insensitiveStartsWith("foobar", "bar"));
        assertFalse(insensitiveStartsWith("foo", "foobar"));
        assertTrue(insensitiveStartsWith("foobar", ""));
        assertTrue(insensitiveStartsWith("foo", "foo"));
        assertTrue(insensitiveStartsWith("FOoBar", "FOo"));
        assertFalse(insensitiveStartsWith("", "bar"));
        assertNull(insensitiveStartsWith(null, "bar"));
        assertNull(insensitiveStartsWith("foo", null));
        assertNull(insensitiveStartsWith(null, null));

        assertTrue(insensitiveStartsWith("foo", "FO"));
        assertTrue(insensitiveStartsWith("foo", "FOo"));
    }

    private Boolean insensitiveStartsWith(String left, String right) {
        return startsWith(true, left, right);
    }

    private Boolean startsWith(boolean caseInsensitive, String left, String right) {
        return (Boolean) new StartsWithFunctionPipeTests.StartsWithTest(EMPTY, l(left), l(right), caseInsensitive).makePipe()
            .asProcessor()
            .process(null);
    }

    private Boolean untypedStartsWith(Object left, Object right) {
        return (Boolean) new StartsWithFunctionPipeTests.StartsWithTest(EMPTY, l(left), l(right), randomBoolean()).makePipe()
            .asProcessor()
            .process(null);
    }

    public void testStartsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> untypedStartsWith(5, "foo"));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class, () -> untypedStartsWith("bar", false));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testStartsWithFunctionWithRandomInvalidDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class, () -> untypedStartsWith(literal, "foo"));
        assertThat(siae.getMessage(), Matchers.startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class, () -> untypedStartsWith("foo", literal));
        assertThat(siae.getMessage(), Matchers.startsWith("A string/char is required; received"));
    }
}
