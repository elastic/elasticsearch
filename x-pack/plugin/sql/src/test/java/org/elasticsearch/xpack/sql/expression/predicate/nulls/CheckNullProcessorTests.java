/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.nulls;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

public class CheckNullProcessorTests extends AbstractWireSerializingTestCase<CheckNullProcessor> {

    public static CheckNullProcessor randomProcessor() {
        return new CheckNullProcessor(randomFrom(CheckNullProcessor.CheckNullOperation.values()));
    }

    @Override
    protected CheckNullProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<CheckNullProcessor> instanceReader() {
        return CheckNullProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testIsNull() {
        assertEquals(true, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NULL).process(null));
        assertEquals(false, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NULL).process("foo"));
        assertEquals(false, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NULL).process(1));
    }

    public void testIsNotNull() {
        assertEquals(false, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NOT_NULL).process(null));
        assertEquals(true, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NOT_NULL).process("foo"));
        assertEquals(true, new CheckNullProcessor(CheckNullProcessor.CheckNullOperation.IS_NOT_NULL).process(1));
    }
}
