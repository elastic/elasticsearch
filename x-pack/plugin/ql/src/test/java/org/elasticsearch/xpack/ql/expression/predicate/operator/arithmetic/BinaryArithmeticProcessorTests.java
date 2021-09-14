/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class BinaryArithmeticProcessorTests extends AbstractWireSerializingTestCase<BinaryArithmeticProcessor> {
    public static BinaryArithmeticProcessor randomProcessor() {
        return new BinaryArithmeticProcessor(
                new ConstantProcessor(randomLong()),
                new ConstantProcessor(randomLong()),
                randomFrom(DefaultBinaryArithmeticOperation.values()));
    }

    @Override
    protected BinaryArithmeticProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<BinaryArithmeticProcessor> instanceReader() {
        return BinaryArithmeticProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testAdd() {
        Processor ba = new Add(EMPTY, l(7), l(3)).makePipe().asProcessor();
        assertEquals(10, ba.process(null));
    }

    public void testSub() {
        Processor ba = new Sub(EMPTY, l(7), l(3)).makePipe().asProcessor();
        assertEquals(4, ba.process(null));
    }

    public void testMul() {
        Processor ba = new Mul(EMPTY, l(7), l(3)).makePipe().asProcessor();
        assertEquals(21, ba.process(null));
    }

    public void testDiv() {
        Processor ba = new Div(EMPTY, l(7), l(3)).makePipe().asProcessor();
        assertEquals(2, ((Number) ba.process(null)).longValue());
        ba = new Div(EMPTY, l((double) 7), l(3)).makePipe().asProcessor();
        assertEquals(2.33, ((Number) ba.process(null)).doubleValue(), 0.01d);
    }

    public void testMod() {
        Processor ba = new Mod(EMPTY, l(7), l(3)).makePipe().asProcessor();
        assertEquals(1, ba.process(null));
    }

    public void testNegate() {
        Processor ba = new Neg(EMPTY, l(7)).asPipe().asProcessor();
        assertEquals(-7, ba.process(null));
    }

    // ((3*2+4)/2-2)%2
    public void testTree() {
        Expression mul = new Mul(EMPTY, l(3), l(2));
        Expression add = new Add(EMPTY, mul, l(4));
        Expression div = new Div(EMPTY, add, l(2));
        Expression sub = new Sub(EMPTY, div, l(2));
        Mod mod = new Mod(EMPTY, sub, l(2));

        Processor proc = mod.makePipe().asProcessor();
        assertEquals(1, proc.process(null));
    }

    public void testHandleNull() {
        assertNull(new Add(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Sub(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Mul(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Div(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Mod(EMPTY, l(null), l(3)).makePipe().asProcessor().process(null));
        assertNull(new Neg(EMPTY, l(null)).makePipe().asProcessor().process(null));
    }

    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
