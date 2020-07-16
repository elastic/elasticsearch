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

import static org.elasticsearch.xpack.ql.TestUtils.equalsOf;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOf;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.ql.TestUtils.lessThanOf;
import static org.elasticsearch.xpack.ql.TestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.ql.TestUtils.notEqualsOf;
import static org.elasticsearch.xpack.ql.TestUtils.nullEqualsOf;
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
        assertEquals(true, equalsOf(l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, equalsOf(l(3), l(4)).makePipe().asProcessor().process(null));
    }

    public void testNullEq() {
        assertEquals(true, nullEqualsOf(l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, nullEqualsOf(l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, nullEqualsOf(NULL, NULL).makePipe().asProcessor().process(null));
        assertEquals(false, nullEqualsOf(l(4), NULL).makePipe().asProcessor().process(null));
        assertEquals(false, nullEqualsOf(NULL, l(4)).makePipe().asProcessor().process(null));
    }

    public void testNEq() {
        assertEquals(false, notEqualsOf(l(4), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, notEqualsOf(l(3), l(4)).makePipe().asProcessor().process(null));
    }

    public void testGt() {
        assertEquals(true, greaterThanOf(l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(false, greaterThanOf(l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, greaterThanOf(l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testGte() {
        assertEquals(true, greaterThanOrEqualOf(l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(false, greaterThanOrEqualOf(l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, greaterThanOrEqualOf(l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testLt() {
        assertEquals(false, lessThanOf(l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(true, lessThanOf(l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(false, lessThanOf(l(3), l(3)).makePipe().asProcessor().process(null));
    }

    public void testLte() {
        assertEquals(false, lessThanOrEqualOf(l(4), l(3)).makePipe().asProcessor().process(null));
        assertEquals(true, lessThanOrEqualOf(l(3), l(4)).makePipe().asProcessor().process(null));
        assertEquals(true, lessThanOrEqualOf(l(3), l(3)).makePipe().asProcessor().process(null));
    }
    
    public void testHandleNull() {
        assertNull(equalsOf(NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(notEqualsOf(NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(greaterThanOf(NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(greaterThanOrEqualOf(NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(lessThanOf(NULL, l(3)).makePipe().asProcessor().process(null));
        assertNull(lessThanOrEqualOf(NULL, l(3)).makePipe().asProcessor().process(null));
    }
    
    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
