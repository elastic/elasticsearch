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
import org.elasticsearch.xpack.ql.session.Configuration;

import static org.elasticsearch.xpack.eql.EqlTestUtils.randomConfigurationWithCaseSensitive;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.startsWith;

public class EndsWithFunctionProcessorTests extends ESTestCase {

    final Configuration caseInsensitive = randomConfigurationWithCaseSensitive(false);

    public void testEndsWithFunctionWithValidInputCaseSensitive() {
        final Configuration caseSensitive = randomConfigurationWithCaseSensitive(true);
        assertEquals(true, new EndsWith(EMPTY, l("foobarbar"), l("r"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foobarbar"), l("R"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foobarbar"), l("bar"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foobarBar"), l("bar"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foobar"), l("foo"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foo"), l("foobar"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foobar"), l(""), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("foo"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foo"), l("oO"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foo"), l("FOo"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l('f'), l('f'), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l(""), l("bar"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l("bar"), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l("foo"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l(null), caseSensitive).makePipe().asProcessor().process(null));
    }

    public void testEndsWithFunctionWithValidInputCaseInsensitive() {
        assertEquals(true, new EndsWith(EMPTY, l("foobarbar"), l("r"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foobarbar"), l("R"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foobar"), l("foo"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l("foo"), l("foobar"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foobar"), l(""), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("foo"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("oO"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l("foo"), l("FOo"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(true, new EndsWith(EMPTY, l('f'), l('f'), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(false, new EndsWith(EMPTY, l(""), l("bar"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l("bar"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l("foo"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new EndsWith(EMPTY, l(null), l(null), caseInsensitive).makePipe().asProcessor().process(null));
    }

    public void testEndsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l(5), l("foo"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l("bar"), l(false), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testEndsWithFunctionWithRandomInvalidDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, literal, l("foo"), caseInsensitive).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new EndsWith(EMPTY, l("foo"), literal, caseInsensitive).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
    }
}
