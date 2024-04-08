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
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class BinaryStringNumericProcessorTests extends AbstractWireSerializingTestCase<BinaryStringNumericProcessor> {

    @Override
    protected BinaryStringNumericProcessor createTestInstance() {
        return new BinaryStringNumericProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(1, 128)),
            new ConstantProcessor(randomInt(256)),
            randomFrom(BinaryStringNumericOperation.values())
        );
    }

    @Override
    protected BinaryStringNumericProcessor mutateInstance(BinaryStringNumericProcessor instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<BinaryStringNumericProcessor> instanceReader() {
        return BinaryStringNumericProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testLeftFunctionWithValidInput() {
        assertEquals("foo", new Left(EMPTY, l("foo bar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("foo bar", new Left(EMPTY, l("foo bar"), l(7)).makePipe().asProcessor().process(null));
        assertEquals("foo bar", new Left(EMPTY, l("foo bar"), l(123)).makePipe().asProcessor().process(null));
        assertEquals("f", new Left(EMPTY, l('f'), l(1)).makePipe().asProcessor().process(null));
    }

    public void testLeftFunctionWithEdgeCases() {
        assertNull(new Left(EMPTY, l("foo bar"), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Left(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Left(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l("foo bar"), l(-1)).makePipe().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l("foo bar"), l(0)).makePipe().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l('f'), l(0)).makePipe().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l('f'), l(Integer.MIN_VALUE)).makePipe().asProcessor().process(null));
    }

    public void testLeftFunctionInputValidation() {
        Exception e = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Left(EMPTY, l(5), l(3)).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [5]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Left(EMPTY, l("foo bar"), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A fixed point number is required for [count]; received [java.lang.String]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Left(EMPTY, l("foo"), l((long) Integer.MIN_VALUE - 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [-2147483649]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Left(EMPTY, l("foo"), l((long) Integer.MAX_VALUE + 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [2147483648]", e.getMessage());

        e = expectThrows(InvalidArgumentException.class, () -> new Left(EMPTY, l("foo"), l(1.0)).makePipe().asProcessor().process(null));
        assertEquals("A fixed point number is required for [count]; received [java.lang.Double]", e.getMessage());
    }

    public void testRightFunctionWithValidInput() {
        assertEquals("bar", new Right(EMPTY, l("foo bar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("foo bar", new Right(EMPTY, l("foo bar"), l(7)).makePipe().asProcessor().process(null));
        assertEquals("foo bar", new Right(EMPTY, l("foo bar"), l(123)).makePipe().asProcessor().process(null));
        assertEquals("f", new Right(EMPTY, l('f'), l(1)).makePipe().asProcessor().process(null));
    }

    public void testRightFunctionWithEdgeCases() {
        assertNull(new Right(EMPTY, l("foo bar"), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Right(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Right(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l("foo bar"), l(-1)).makePipe().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l("foo bar"), l(0)).makePipe().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l('f'), l(0)).makePipe().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l('f'), l(Integer.MIN_VALUE)).makePipe().asProcessor().process(null));
    }

    public void testRightFunctionInputValidation() {
        Exception e = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Right(EMPTY, l(5), l(3)).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [5]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Right(EMPTY, l("foo bar"), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A fixed point number is required for [count]; received [java.lang.String]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Right(EMPTY, l("foo"), l((long) Integer.MIN_VALUE - 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [-2147483649]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Right(EMPTY, l("foo"), l((long) Integer.MAX_VALUE + 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [2147483648]", e.getMessage());

        e = expectThrows(InvalidArgumentException.class, () -> new Right(EMPTY, l("foo"), l(1.0)).makePipe().asProcessor().process(null));
        assertEquals("A fixed point number is required for [count]; received [java.lang.Double]", e.getMessage());
    }

    public void testRepeatFunctionWithValidInput() {
        assertEquals("foofoofoo", new Repeat(EMPTY, l("foo"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("foo", new Repeat(EMPTY, l("foo"), l(1)).makePipe().asProcessor().process(null));
        assertEquals("fff", new Repeat(EMPTY, l('f'), l(3)).makePipe().asProcessor().process(null));
    }

    public void testRepeatFunctionWithEdgeCases() {
        assertNull(new Repeat(EMPTY, l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l("foo"), l(-1)).makePipe().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l("foo"), l(0)).makePipe().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l('f'), l(Integer.MIN_VALUE)).makePipe().asProcessor().process(null));
    }

    public void testRepeatFunctionInputsValidation() {
        Exception e = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Repeat(EMPTY, l(5), l(3)).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [5]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Repeat(EMPTY, l("foo bar"), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A fixed point number is required for [count]; received [java.lang.String]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Repeat(EMPTY, l("foo"), l((long) Integer.MIN_VALUE - 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [-2147483649]", e.getMessage());

        e = expectThrows(
            InvalidArgumentException.class,
            () -> new Repeat(EMPTY, l("foo"), l((long) Integer.MAX_VALUE + 1)).makePipe().asProcessor().process(null)
        );
        assertEquals("[count] out of the allowed range [-2147483648, 2147483647], received [2147483648]", e.getMessage());

        e = expectThrows(InvalidArgumentException.class, () -> new Repeat(EMPTY, l("foo"), l(1.0)).makePipe().asProcessor().process(null));
        assertEquals("A fixed point number is required for [count]; received [java.lang.Double]", e.getMessage());
    }
}
