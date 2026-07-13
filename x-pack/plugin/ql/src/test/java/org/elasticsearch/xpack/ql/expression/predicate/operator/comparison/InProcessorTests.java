/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

import java.util.Arrays;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InProcessorTests extends AbstractWireSerializingTestCase<InProcessor> {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);

    public static InProcessor randomProcessor() {
        return new InProcessor(Arrays.asList(new ConstantProcessor(randomLong()), new ConstantProcessor(randomLong())));
    }

    @Override
    protected InProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected InProcessor mutateInstance(InProcessor instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<InProcessor> instanceReader() {
        return InProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testEq() {
        assertEquals(true, new In(EMPTY, TWO, Arrays.asList(ONE, TWO, THREE)).makePipe().asProcessor().process(null));
        assertEquals(false, new In(EMPTY, THREE, Arrays.asList(ONE, TWO)).makePipe().asProcessor().process(null));
    }

    public void testHandleNullOnLeftValue() {
        assertNull(new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE)).makePipe().asProcessor().process(null));
        assertNull(new In(EMPTY, NULL, Arrays.asList(ONE, NULL, TWO)).makePipe().asProcessor().process(null));
    }

    public void testHandleNullOnRightValue() {
        assertEquals(true, new In(EMPTY, THREE, Arrays.asList(ONE, NULL, THREE)).makePipe().asProcessor().process(null));
        assertNull(new In(EMPTY, TWO, Arrays.asList(ONE, NULL, THREE)).makePipe().asProcessor().process(null));
    }

    private static Literal L(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
