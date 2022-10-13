/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

import static org.elasticsearch.xpack.eql.EqlTestUtils.seq;
import static org.elasticsearch.xpack.eql.EqlTestUtils.sneq;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InsensitiveBinaryComparisonProcessorTests extends AbstractWireSerializingTestCase<InsensitiveBinaryComparisonProcessor> {
    public static InsensitiveBinaryComparisonProcessor randomProcessor() {
        return new InsensitiveBinaryComparisonProcessor(
            new ConstantProcessor(randomLong()),
            new ConstantProcessor(randomLong()),
            randomFrom(InsensitiveBinaryComparisonProcessor.InsensitiveBinaryComparisonOperation.values())
        );
    }

    @Override
    protected InsensitiveBinaryComparisonProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<InsensitiveBinaryComparisonProcessor> instanceReader() {
        return InsensitiveBinaryComparisonProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testStringEq() {
        assertEquals(true, p(seq(l("a"), l("a"))));
        assertEquals(true, p(seq(l("A"), l("a"))));
        assertEquals(true, p(seq(l("aBcD"), l("AbCd"))));
        assertEquals(true, p(seq(l("abc"), l("abc"))));

        assertEquals(false, p(seq(l("abc"), l("cba"))));
    }

    public void testNonStringArguments() {
        expectThrows(EqlIllegalArgumentException.class, () -> p(seq(l(12), l(12))));
        expectThrows(EqlIllegalArgumentException.class, () -> p(seq(l(12), l("12"))));
        expectThrows(EqlIllegalArgumentException.class, () -> p(seq(l("12"), l(12))));
    }

    public void testNullStringEquals() {
        assertNull(p(seq(l(null), l(null))));
        assertNull(p(seq(l("a"), l(null))));
        assertNull(p(seq(l(null), l("a"))));
    }

    public void testStringNotEquals() {
        assertEquals(false, p(sneq(l("a"), l("a"))));
        assertEquals(false, p(sneq(l("A"), l("a"))));
        assertEquals(false, p(sneq(l("aBcD"), l("AbCd"))));
        assertEquals(false, p(sneq(l("abc"), l("abc"))));

        assertEquals(true, p(sneq(l("abc"), l("cba"))));
    }

    public void testNullStringNotEquals() {
        assertNull(p(sneq(l(null), l(null))));
        assertNull(p(sneq(l("a"), l(null))));
        assertNull(p(sneq(l(null), l("a"))));
    }

    public void testRegularNotEquals() {
        expectThrows(EqlIllegalArgumentException.class, () -> p(sneq(l(12), l(12))));
        expectThrows(EqlIllegalArgumentException.class, () -> p(sneq(l(12), l("12"))));
        expectThrows(EqlIllegalArgumentException.class, () -> p(sneq(l("12"), l(12))));
    }

    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }

    private static Object p(InsensitiveBinaryComparison ibc) {
        return ibc.makePipe().asProcessor().process(null);
    }
}
