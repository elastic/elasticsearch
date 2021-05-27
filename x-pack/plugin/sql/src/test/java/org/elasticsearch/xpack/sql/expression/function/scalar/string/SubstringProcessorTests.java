/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class SubstringProcessorTests extends AbstractWireSerializingTestCase<SubstringFunctionProcessor> {

    @Override
    protected SubstringFunctionProcessor createTestInstance() {
        return new SubstringFunctionProcessor(
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 256)),
                new ConstantProcessor(randomInt(256)),
                new ConstantProcessor(randomInt(256)));
    }

    @Override
    protected Reader<SubstringFunctionProcessor> instanceReader() {
        return SubstringFunctionProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testSubstringFunctionWithValidInput() {
        assertEquals("bar", new Substring(EMPTY, l("foobarbar"), l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals("foo", new Substring(EMPTY, l("foobarbar"), l(1), l(3)).makePipe().asProcessor().process(null));
        assertEquals("baz", new Substring(EMPTY, l("foobarbaz"), l(7), l(3)).makePipe().asProcessor().process(null));
        assertEquals("f", new Substring(EMPTY, l('f'), l(1), l(1)).makePipe().asProcessor().process(null));
    }

    public void testSubstringFunctionWithEdgeCases() {
        assertNull(new Substring(EMPTY, l("foobarbar"), l(1), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Substring(EMPTY, l("foobarbar"), l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Substring(EMPTY, l(null), l(1), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Substring(EMPTY, l(null), l(null), l(null)).makePipe().asProcessor().process(null));

        assertEquals("foo", new Substring(EMPTY, l("foobarbar"), l(-5), l(3)).makePipe().asProcessor().process(null));
        assertEquals("barbar", new Substring(EMPTY, l("foobarbar"), l(4), l(30)).makePipe().asProcessor().process(null));
        assertEquals("r", new Substring(EMPTY, l("foobarbar"), l(9), l(1)).makePipe().asProcessor().process(null));
        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(10), l(1)).makePipe().asProcessor().process(null));
        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(123), l(3)).makePipe().asProcessor().process(null));
    }

    public void testSubstringFunctionInputsValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Substring(EMPTY, l(5), l(1), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Substring(EMPTY, l("foobarbar"), l(1), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("A fixed point number is required for [length]; received [java.lang.String]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Substring(EMPTY, l("foobarbar"), l("bar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A fixed point number is required for [start]; received [java.lang.String]", siae.getMessage());

        assertEquals("f", new Substring(EMPTY, l("foobarbar"), l(Integer.MIN_VALUE + 1), l(1)).makePipe().asProcessor().process(null));
        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("foobarbar"), l(Integer.MIN_VALUE), l(1)).makePipe().asProcessor().process(null));
        assertEquals("[start] out of the allowed range [-2147483647, 2147483647], received [-2147483648]", siae.getMessage());

        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(Integer.MAX_VALUE), l(1)).makePipe().asProcessor().process(null));
        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("foobarbar"), l((long) Integer.MAX_VALUE + 1), l(1)).makePipe().asProcessor().process(null));
        assertEquals("[start] out of the allowed range [-2147483647, 2147483647], received [2147483648]", siae.getMessage());

        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(1), l(0)).makePipe().asProcessor().process(null));
        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("foobarbar"), l(1), l(-1)).makePipe().asProcessor().process(null));
        assertEquals("[length] out of the allowed range [0, 2147483647], received [-1]", siae.getMessage());

        assertEquals("foobarbar", new Substring(EMPTY, l("foobarbar"), l(1), l(Integer.MAX_VALUE)).makePipe().asProcessor().process(null));
        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new Substring(EMPTY, l("foobarbar"), l(1), l((long) Integer.MAX_VALUE + 1)).makePipe().asProcessor().process(null));
        assertEquals("[length] out of the allowed range [0, 2147483647], received [2147483648]", siae.getMessage());
    }
}
