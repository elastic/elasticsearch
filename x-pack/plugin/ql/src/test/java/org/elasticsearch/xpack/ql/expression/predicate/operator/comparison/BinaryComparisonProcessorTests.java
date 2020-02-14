/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class BinaryComparisonProcessorTests extends AbstractWireSerializingTestCase<BinaryComparisonProcessor> {
    public static BinaryComparisonProcessor randomProcessor() {
        return new BinaryComparisonProcessor(
                new ConstantProcessor(randomLong()),
                new ConstantProcessor(randomLong()),
                randomFrom(BinaryComparisonProcessor.BinaryComparisonOperation.values()));
    }

    @Override
    protected BinaryComparisonProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<BinaryComparisonProcessor> instanceReader() {
        return BinaryComparisonProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testEq() {
        assertEquals(true, new Equals(EMPTY, l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, new Equals(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
    }

    public void testNullEq() {
        assertEquals(true, new NullEquals(EMPTY, l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, new NullEquals(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, new NullEquals(EMPTY, NULL, NULL).makePipe().asProcessor().process(null));
        assertEquals(false, new NullEquals(EMPTY, l(4), NULL).makePipe().asProcessor().process(null));
        assertEquals(false, new NullEquals(EMPTY, NULL, l(4)).makePipe().asProcessor().process(null));
    }

    public void testNEq() {
        assertEquals(false, new NotEquals(EMPTY, l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, new NotEquals(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
    }

    public void testGt() {
        assertEquals(true, new GreaterThan(EMPTY, l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(false, new GreaterThan(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, new GreaterThan(EMPTY, l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testGte() {
        assertEquals(true, new GreaterThanOrEqual(EMPTY, l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(false, new GreaterThanOrEqual(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, new GreaterThanOrEqual(EMPTY, l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testLt() {
        assertEquals(false, new LessThan(EMPTY, l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(true, new LessThan(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, new LessThan(EMPTY, l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testLte() {
        assertEquals(false, new LessThanOrEqual(EMPTY, l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(true, new LessThanOrEqual(EMPTY, l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, new LessThanOrEqual(EMPTY, l(3), l(3)).makePipe().asProcessor().process(null));
    }
    
    public void testHandleNull() {
        assertNull(new Equals(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(new NotEquals(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(new GreaterThan(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(new GreaterThanOrEqual(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(new LessThan(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(new LessThanOrEqual(EMPTY, NULL, l(3)).makePipe().asProcessor().process(null));
    }
    
    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
