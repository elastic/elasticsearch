/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class BinaryMathProcessorTests extends AbstractWireSerializingTestCase<BinaryMathProcessor> {
    public static BinaryMathProcessor randomProcessor() {
        return new BinaryMathProcessor(
            new ConstantProcessor(randomLong()),
            new ConstantProcessor(randomLong()),
            randomFrom(BinaryMathProcessor.BinaryMathOperation.values())
        );
    }

    @Override
    protected BinaryMathProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected BinaryMathProcessor mutateInstance(BinaryMathProcessor instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<BinaryMathProcessor> instanceReader() {
        return BinaryMathProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testAtan2() {
        Processor ba = new ATan2(EMPTY, l(1), l(1)).makePipe().asProcessor();
        assertEquals(0.7853981633974483d, ba.process(null));
    }

    public void testPower() {
        Processor ba = new Power(EMPTY, l(2), l(2)).makePipe().asProcessor();
        assertEquals(4d, ba.process(null));
    }

    public void testRoundWithValidInput() {
        assertEquals(123.5, new Round(EMPTY, l(123.45), l(1)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Round(EMPTY, l(123.45), l(0)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Round(EMPTY, l(123.45), null).makePipe().asProcessor().process(null));
        assertEquals(123L, new Round(EMPTY, l(123L), l(0)).makePipe().asProcessor().process(null));
        assertEquals(123L, new Round(EMPTY, l(123L), l(5)).makePipe().asProcessor().process(null));
        assertEquals(120L, new Round(EMPTY, l(123L), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(100L, new Round(EMPTY, l(123L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(0L, new Round(EMPTY, l(123L), l(-3)).makePipe().asProcessor().process(null));
        assertEquals(0L, new Round(EMPTY, l(123L), l(-100)).makePipe().asProcessor().process(null));
        assertEquals(1000L, new Round(EMPTY, l(999L), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(1000.0, new Round(EMPTY, l(999.0), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(130L, new Round(EMPTY, l(125L), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(12400L, new Round(EMPTY, l(12350L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(12400.0, new Round(EMPTY, l(12350.0), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(12300.0, new Round(EMPTY, l(12349.0), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-12300L, new Round(EMPTY, l(-12349L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-12400L, new Round(EMPTY, l(-12350L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-12400.0, new Round(EMPTY, l(-12350.0), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-100L, new Round(EMPTY, l(-123L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-120.0, new Round(EMPTY, l(-123.45), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(-123.5, new Round(EMPTY, l(-123.45), l(1)).makePipe().asProcessor().process(null));
        assertEquals(-124.0, new Round(EMPTY, l(-123.5), l(0)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Round(EMPTY, l(-123.45), null).makePipe().asProcessor().process(null));
    }

    public void testRoundFunctionWithEdgeCasesInputs() {
        assertNull(new Round(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(123.456, new Round(EMPTY, l(123.456), l(Integer.MAX_VALUE)).makePipe().asProcessor().process(null));
        assertEquals(0.0, new Round(EMPTY, l(123.456), l(Integer.MIN_VALUE)).makePipe().asProcessor().process(null));
        assertEquals(0L, new Round(EMPTY, l(0L), l(0)).makePipe().asProcessor().process(null));
        assertEquals(0, new Round(EMPTY, l(0), l(0)).makePipe().asProcessor().process(null));
        assertEquals((short) 0, new Round(EMPTY, l((short) 0), l(0)).makePipe().asProcessor().process(null));
        assertEquals((byte) 0, new Round(EMPTY, l((byte) 0), l(0)).makePipe().asProcessor().process(null));
        assertEquals(Long.MAX_VALUE, new Round(EMPTY, l(Long.MAX_VALUE), null).makePipe().asProcessor().process(null));
        assertEquals(Long.MAX_VALUE, new Round(EMPTY, l(Long.MAX_VALUE), l(5)).makePipe().asProcessor().process(null));
        assertEquals(Long.MIN_VALUE, new Round(EMPTY, l(Long.MIN_VALUE), null).makePipe().asProcessor().process(null));
        assertEquals(Long.MIN_VALUE, new Round(EMPTY, l(Long.MIN_VALUE), l(5)).makePipe().asProcessor().process(null));
        // absolute precision at the extremes
        assertEquals(9223372036854775800L, new Round(EMPTY, l(Long.MAX_VALUE), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-9223372036854775800L, new Round(EMPTY, l(Long.MIN_VALUE), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-9223372036854775800L, new Round(EMPTY, l(Long.MIN_VALUE + 1), l(-2)).makePipe().asProcessor().process(null));
        // overflows
        expectThrows(ArithmeticException.class, () -> new Round(EMPTY, l(Long.MAX_VALUE), l(-3)).makePipe().asProcessor().process(null));
        expectThrows(ArithmeticException.class, () -> new Round(EMPTY, l(Long.MIN_VALUE), l(-3)).makePipe().asProcessor().process(null));
        expectThrows(ArithmeticException.class, () -> new Round(EMPTY, l(Integer.MAX_VALUE), l(-3)).makePipe().asProcessor().process(null));
        expectThrows(ArithmeticException.class, () -> new Round(EMPTY, l(Integer.MIN_VALUE), l(-3)).makePipe().asProcessor().process(null));
        // very big numbers, ie. overflow with Long rounding
        assertEquals(1234456.234567, new Round(EMPTY, l(1234456.234567), l(20)).makePipe().asProcessor().process(null));
        assertEquals(12344561234567456.2345, new Round(EMPTY, l(12344561234567456.234567), l(4)).makePipe().asProcessor().process(null));
        assertEquals(12344561234567000., new Round(EMPTY, l(12344561234567456.234567), l(-3)).makePipe().asProcessor().process(null));
    }

    public void testRoundInputValidation() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Round(EMPTY, l(5), l("foobarbar")).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [foobarbar]", siae.getMessage());
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Round(EMPTY, l("bla"), l(0)).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [bla]", siae.getMessage());
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Round(EMPTY, l(123.34), l(0.1)).makePipe().asProcessor().process(null)
        );
        assertEquals("An integer number is required; received [0.1] as second parameter", siae.getMessage());
    }

    public void testTruncateWithValidInput() {
        assertEquals(123L, new Truncate(EMPTY, l(123L), l(3)).makePipe().asProcessor().process(null));
        assertEquals(123L, new Truncate(EMPTY, l(123L), l(0)).makePipe().asProcessor().process(null));
        assertEquals(120L, new Truncate(EMPTY, l(123L), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(0L, new Truncate(EMPTY, l(123L), l(-3)).makePipe().asProcessor().process(null));
        assertEquals(123.4, new Truncate(EMPTY, l(123.45), l(1)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Truncate(EMPTY, l(123.45), l(0)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Truncate(EMPTY, l(123.45), null).makePipe().asProcessor().process(null));
        assertEquals((byte) -100, new Truncate(EMPTY, l((byte) -123), l(-2)).makePipe().asProcessor().process(null));
        assertEquals((short) -100, new Truncate(EMPTY, l((short) -123), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-100, new Truncate(EMPTY, l(-123), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-100L, new Truncate(EMPTY, l(-123L), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-120.0, new Truncate(EMPTY, l(-123.45), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Truncate(EMPTY, l(-123.5), l(0)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Truncate(EMPTY, l(-123.45), null).makePipe().asProcessor().process(null));
    }

    public void testTruncateFunctionWithEdgeCasesInputs() {
        assertNull(new Truncate(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(0, new Truncate(EMPTY, l(0), l(0)).makePipe().asProcessor().process(null));
        assertEquals(0L, new Truncate(EMPTY, l(0L), l(0)).makePipe().asProcessor().process(null));
        assertEquals(Long.MAX_VALUE, new Truncate(EMPTY, l(Long.MAX_VALUE), l(0)).makePipe().asProcessor().process(null));
        assertEquals(9223372036854775800L, new Truncate(EMPTY, l(Long.MAX_VALUE), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(-9223372036854775800L, new Truncate(EMPTY, l(Long.MIN_VALUE), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(Double.NaN, new Truncate(EMPTY, l(123.456), l(Integer.MAX_VALUE)).makePipe().asProcessor().process(null));
    }

    public void testTruncateInputValidation() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Truncate(EMPTY, l(5), l("foobarbar")).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [foobarbar]", siae.getMessage());
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Truncate(EMPTY, l("bla"), l(0)).makePipe().asProcessor().process(null)
        );
        assertEquals("A number is required; received [bla]", siae.getMessage());
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Truncate(EMPTY, l(123.34), l(0.1)).makePipe().asProcessor().process(null)
        );
        assertEquals("An integer number is required; received [0.1] as second parameter", siae.getMessage());
    }

    public void testHandleNull() {
        assertNull(new ATan2(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Power(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
    }

    private static Literal l(Object value) {
        return SqlTestUtils.literal(value);
    }
}
