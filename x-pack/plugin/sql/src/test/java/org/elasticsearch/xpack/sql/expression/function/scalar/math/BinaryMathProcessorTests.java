/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

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
        Processor ba = new ATan2(EMPTY, l(1), l(1)).makeProcessorDefinition().asProcessor();
        assertEquals(0.7853981633974483d, ba.process(null));
    }

    public void testPower() {
        Processor ba = new Power(EMPTY, l(2), l(2)).makeProcessorDefinition().asProcessor();
        assertEquals(4d, ba.process(null));
    }

    public void testHandleNull() {
        assertNull(new ATan2(EMPTY, l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Power(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
    }
    
    private static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }
}