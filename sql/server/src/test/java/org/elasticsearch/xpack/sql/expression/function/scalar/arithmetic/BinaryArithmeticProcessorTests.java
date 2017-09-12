/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class BinaryArithmeticProcessorTests extends AbstractWireSerializingTestCase<BinaryArithmeticProcessor> {
    public static BinaryArithmeticProcessor randomProcessor() {
        return new BinaryArithmeticProcessor(
                new ConstantProcessor(randomLong()),
                new ConstantProcessor(randomLong()), 
                randomFrom(BinaryArithmeticProcessor.BinaryArithmeticOperation.values()));
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
        BinaryArithmeticProcessor ba = new Add(EMPTY, l(7), l(3)).makeProcessor().asProcessor();
        assertEquals(10, ba.process(null));
    }

    public void testSub() {
        BinaryArithmeticProcessor ba = new Sub(EMPTY, l(7), l(3)).makeProcessor().asProcessor();
        assertEquals(4, ba.process(null));
    }

    public void testMul() {
        BinaryArithmeticProcessor ba = new Mul(EMPTY, l(7), l(3)).makeProcessor().asProcessor();
        assertEquals(21, ba.process(null));
    }

    public void testDiv() {
        BinaryArithmeticProcessor ba = new Div(EMPTY, l(7), l(3)).makeProcessor().asProcessor();
        assertEquals(2, ((Number) ba.process(null)).longValue());
        ba = new Div(EMPTY, l((double) 7), l(3)).makeProcessor().asProcessor();
        assertEquals(2.33, ((Number) ba.process(null)).doubleValue(), 0.01d);
    }

    public void testMod() {
        BinaryArithmeticProcessor ba = new Mod(EMPTY, l(7), l(3)).makeProcessor().asProcessor();
        assertEquals(1, ba.process(null));
    }

    public void testNegate() {
        Processor ba = new Neg(EMPTY, l(7)).asProcessor().asProcessor();
        assertEquals(-7, ba.process(null));
    }
    
    // ((3*2+4)/2-2)%2
    public void testTree() {
        Expression mul = new Mul(EMPTY, l(3), l(2));
        Expression add = new Add(EMPTY, mul, l(4));
        Expression div = new Div(EMPTY, add, l(2));
        Expression sub = new Sub(EMPTY, div, l(2));
        Mod mod = new Mod(EMPTY, sub, l(2));
        
        Processor proc = mod.makeProcessor().asProcessor();
        assertEquals(1, proc.process(null));
    }
    
    private static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }
}