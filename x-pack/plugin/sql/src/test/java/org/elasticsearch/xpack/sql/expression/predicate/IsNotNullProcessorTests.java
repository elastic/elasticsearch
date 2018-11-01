/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class IsNotNullProcessorTests extends AbstractWireSerializingTestCase<IsNotNullProcessor> {

    public static IsNotNullProcessor randomProcessor() {
        return IsNotNullProcessor.INSTANCE;
    }

    @Override
    protected IsNotNullProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<IsNotNullProcessor> instanceReader() {
        return IsNotNullProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testNullAndNotNull() {
        assertEquals(true, new IsNotNull(EMPTY, Literal.of(EMPTY, "foo")).makePipe().asProcessor().process(null));
        assertEquals(false, new IsNotNull(EMPTY, Literal.NULL).makePipe().asProcessor().process(null));
    }

    public void testHandleNaN() {
        assertFalse(IsNotNullProcessor.apply(Double.NaN));
        assertFalse(IsNotNullProcessor.apply(Float.NaN));
    }

    private static Literal L(Object value) {
        return Literal.of(EMPTY, value);
    }
}
