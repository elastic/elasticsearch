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
                randomFrom(BinaryMathProcessor.BinaryMathOperation.values()));
    }

    @Override
    protected BinaryMathProcessor createTestInstance() {
        return randomProcessor();
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
        assertEquals(123.0, new Round(EMPTY, l(123), l(3)).makePipe().asProcessor().process(null));
        assertEquals(123.5, new Round(EMPTY, l(123.45), l(1)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Round(EMPTY, l(123.45), l(0)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Round(EMPTY, l(123.45), null).makePipe().asProcessor().process(null));
        assertEquals(-100.0, new Round(EMPTY, l(-123), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-120.0, new Round(EMPTY, l(-123.45), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(-124.0, new Round(EMPTY, l(-123.5), l(0)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Round(EMPTY, l(-123.45), null).makePipe().asProcessor().process(null));
    }

    public void testRoundFunctionWithEdgeCasesInputs() {
        assertNull(new Round(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(-0.0, new Round(EMPTY, l(0), l(0)).makePipe().asProcessor().process(null));
        assertEquals((double) Long.MAX_VALUE, new Round(EMPTY, l(Long.MAX_VALUE), l(0))
                .makePipe().asProcessor().process(null));
        assertEquals(0.0, new Round(EMPTY, l(123.456), l(Integer.MAX_VALUE)).makePipe().asProcessor().process(null));
    }

    public void testRoundInputValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Round(EMPTY, l(5), l("foobarbar")).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [foobarbar]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Round(EMPTY, l("bla"), l(0)).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [bla]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Round(EMPTY, l(123.34), l(0.1)).makePipe().asProcessor().process(null));
        assertEquals("An integer number is required; received [0.1] as second parameter", siae.getMessage());
    }

    public void testTruncateWithValidInput() {
        assertEquals(123.0, new Truncate(EMPTY, l(123), l(3)).makePipe().asProcessor().process(null));
        assertEquals(123.4, new Truncate(EMPTY, l(123.45), l(1)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Truncate(EMPTY, l(123.45), l(0)).makePipe().asProcessor().process(null));
        assertEquals(123.0, new Truncate(EMPTY, l(123.45), null).makePipe().asProcessor().process(null));
        assertEquals(-100.0, new Truncate(EMPTY, l(-123), l(-2)).makePipe().asProcessor().process(null));
        assertEquals(-120.0, new Truncate(EMPTY, l(-123.45), l(-1)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Truncate(EMPTY, l(-123.5), l(0)).makePipe().asProcessor().process(null));
        assertEquals(-123.0, new Truncate(EMPTY, l(-123.45), null).makePipe().asProcessor().process(null));
    }

    public void testTruncateFunctionWithEdgeCasesInputs() {
        assertNull(new Truncate(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(0.0, new Truncate(EMPTY, l(0), l(0)).makePipe().asProcessor().process(null));
        assertEquals((double) Long.MAX_VALUE, new Truncate(EMPTY, l(Long.MAX_VALUE), l(0))
                .makePipe().asProcessor().process(null));
        assertEquals(Double.NaN, new Truncate(EMPTY, l(123.456), l(Integer.MAX_VALUE))
                .makePipe().asProcessor().process(null));
    }

    public void testTruncateInputValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Truncate(EMPTY, l(5), l("foobarbar")).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [foobarbar]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Truncate(EMPTY, l("bla"), l(0)).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [bla]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Truncate(EMPTY, l(123.34), l(0.1)).makePipe().asProcessor().process(null));
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
