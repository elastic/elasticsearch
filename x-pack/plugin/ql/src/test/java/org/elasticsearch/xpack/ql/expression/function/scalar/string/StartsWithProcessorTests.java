/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.junit.Before;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.TestUtils.randomConfiguration;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.startsWith;

public class StartsWithProcessorTests extends ESTestCase {

    protected boolean isCaseSensitive;
    protected Configuration config;

    @Before
    public void setup() {
        isCaseSensitive = isCaseSensitiveGenerator().get();
        config = configurationGenerator().get();
    }

    protected Supplier<Boolean> isCaseSensitiveGenerator() {
        return () -> true;
    }

    protected Supplier<Configuration> configurationGenerator() {
        return () -> randomConfiguration();
    }

    protected Supplier<StartsWith> startsWithInstantiator(Source source, Expression field, Expression pattern) {
        return () -> new StartsWith(source, field, pattern, config);
    }

    public void testStartsWithFunctionWithValidInput() {
        assertEquals(true, startsWithInstantiator(EMPTY, l("foobarbar"), l("f")).get().makePipe().asProcessor().process(null));
        assertEquals(false, startsWithInstantiator(EMPTY, l("foobar"), l("bar")).get().makePipe().asProcessor().process(null));
        assertEquals(false, startsWithInstantiator(EMPTY, l("foo"), l("foobar")).get().makePipe().asProcessor().process(null));
        assertEquals(true, startsWithInstantiator(EMPTY, l("foobar"), l("")).get().makePipe().asProcessor().process(null));
        assertEquals(true, startsWithInstantiator(EMPTY, l("foo"), l("foo")).get().makePipe().asProcessor().process(null));
        assertEquals(!isCaseSensitive, startsWithInstantiator(EMPTY, l("foo"), l("FO")).get().makePipe().asProcessor().process(null));
        assertEquals(!isCaseSensitive, startsWithInstantiator(EMPTY, l("foo"), l("FOo")).get().makePipe().asProcessor().process(null));
        assertEquals(true, startsWithInstantiator(EMPTY, l("FOoBar"), l("FOo")).get().makePipe().asProcessor().process(null));
        assertEquals(true, startsWithInstantiator(EMPTY, l('f'), l('f')).get().makePipe().asProcessor().process(null));
        assertEquals(false, startsWithInstantiator(EMPTY, l(""), l("bar")).get().makePipe().asProcessor().process(null));
        assertEquals(null, startsWithInstantiator(EMPTY, l(null), l("bar")).get().makePipe().asProcessor().process(null));
        assertEquals(null, startsWithInstantiator(EMPTY, l("foo"), l(null)).get().makePipe().asProcessor().process(null));
        assertEquals(null, startsWithInstantiator(EMPTY, l(null), l(null)).get().makePipe().asProcessor().process(null));
    }
    
    public void testStartsWithFunctionInputsValidation() {
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> startsWithInstantiator(EMPTY, l(5), l("foo")).get().makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> startsWithInstantiator(EMPTY, l("bar"), l(false)).get().makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [false]", siae.getMessage());
    }

    public void testStartsWithFunctionWithRandomInvalidDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        QlIllegalArgumentException siae = expectThrows(QlIllegalArgumentException.class,
                () -> startsWithInstantiator(EMPTY, literal, l("foo")).get().makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
        siae = expectThrows(QlIllegalArgumentException.class,
                () -> startsWithInstantiator(EMPTY, l("foo"), literal).get().makePipe().asProcessor().process(null));
        assertThat(siae.getMessage(), startsWith("A string/char is required; received"));
    }
}