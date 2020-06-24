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

public class IndexOfFunctionProcessorTests extends ESTestCase {

    final Configuration caseInsensitive = randomConfigurationWithCaseSensitive(false);

    public void testIndexOfFunctionWithValidInputInsensitive() {
        assertEquals(5, new IndexOf(EMPTY, l("foobarbar"), l("r"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(5, new IndexOf(EMPTY, l("foobaRbar"), l("r"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foobar"), l("Foo"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l("foobar"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foo"), l("foo"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(1, new IndexOf(EMPTY, l("foo"), l("oO"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foo"), l("FOo"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l('f'), l('f'), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(""), l("bar"), l(1), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(5, new IndexOf(EMPTY, l("foobarbar"), l("R"), l(5), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(2, new IndexOf(EMPTY, l("foobar"), l("O"), l(2), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foobar"), l("O"), l(3), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(6, new IndexOf(EMPTY, l("foobarbaz"), l("ba"), l(4), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l(null), l(3), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l(null), l(4), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("bar"), l("bar"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
    }

    public void testIndexOfFunctionWithValidInputSensitive() {
        final Configuration caseSensitive = randomConfigurationWithCaseSensitive(true);
        assertEquals(5, new IndexOf(EMPTY, l("foobarbar"), l("r"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(8, new IndexOf(EMPTY, l("foobaRbar"), l("r"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(4, new IndexOf(EMPTY, l("foobARbar"), l("AR"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foobar"), l("foo"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l("foobar"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("foo"), l("foo"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l("oO"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l("FOo"), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l('f'), l('f'), l(null), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(""), l("bar"), l(1), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foobarbar"), l("R"), l(5), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foobar"), l("O"), l(2), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foobar"), l("O"), l(3), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(6, new IndexOf(EMPTY, l("foobarbaz"), l("ba"), l(4), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l("bar"), l(2), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l("foo"), l(null), l(3), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(null, new IndexOf(EMPTY, l(null), l(null), l(4), caseSensitive).makePipe().asProcessor().process(null));
        assertEquals(0, new IndexOf(EMPTY, l("bar"), l("bar"), l(null), caseSensitive).makePipe().asProcessor().process(null));
    }

    public void testIndexOfFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l(5), l("foo"), l(null), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("bar"), l(false), l(2), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("bar"), l("a"), l("1"), caseInsensitive).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [1]", siae.getMessage());
    }

    public void testIndexOfFunctionWithRandomInvalidDataType() {
        Literal stringLiteral = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, stringLiteral, l("foo"), l(1), caseInsensitive).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("foo"), stringLiteral, l(2), caseInsensitive).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        
        Literal numericLiteral = randomValueOtherThanMany(v -> v.dataType().isNumeric(), () -> LiteralTests.randomLiteral());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> new IndexOf(EMPTY, l("foo"), l("o"), numericLiteral, caseInsensitive).makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A number is required; received"));
    }
}
