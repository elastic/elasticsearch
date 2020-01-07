/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.nulls;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

public class CheckNullProcessorTests extends AbstractWireSerializingTestCase<CheckNullProcessor> {

    private static final Processor FALSE = new ConstantProcessor(false);
    private static final Processor TRUE = new ConstantProcessor(true);
    private static final Processor NULL = new ConstantProcessor((Object) null);

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
