/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.processor.Processors;

public class BinaryLogicProcessorTests extends AbstractWireSerializingTestCase<BinaryLogicProcessor> {

    private static final Processor FALSE = new ConstantProcessor(false);
    private static final Processor TRUE = new ConstantProcessor(true);
    private static final Processor NULL = new ConstantProcessor((Object) null);

    public static BinaryLogicProcessor randomProcessor() {
        return new BinaryLogicProcessor(
                new ConstantProcessor(randomFrom(Boolean.FALSE, Boolean.TRUE, null)),
                new ConstantProcessor(randomFrom(Boolean.FALSE, Boolean.TRUE, null)),
                randomFrom(BinaryLogicProcessor.BinaryLogicOperation.values()));
    }

    @Override
    protected BinaryLogicProcessor createTestInstance() {
        return randomProcessor();
    }

    @Override
    protected Reader<BinaryLogicProcessor> instanceReader() {
        return BinaryLogicProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testOR() {
        assertEquals(true, new BinaryLogicProcessor(TRUE, FALSE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertEquals(true, new BinaryLogicProcessor(FALSE, TRUE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertEquals(false, new BinaryLogicProcessor(FALSE, FALSE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertEquals(true, new BinaryLogicProcessor(TRUE, TRUE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
    }

    public void testORNullHandling() {
        assertEquals(true, new BinaryLogicProcessor(TRUE, NULL, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertEquals(true, new BinaryLogicProcessor(NULL, TRUE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertNull(new BinaryLogicProcessor(FALSE, NULL, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertNull(new BinaryLogicProcessor(NULL, FALSE, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
        assertNull(new BinaryLogicProcessor(NULL, NULL, BinaryLogicProcessor.BinaryLogicOperation.OR).process(null));
    }

    public void testAnd() {
        assertEquals(false, new BinaryLogicProcessor(TRUE, FALSE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertEquals(false, new BinaryLogicProcessor(FALSE, TRUE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertEquals(false, new BinaryLogicProcessor(FALSE, FALSE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertEquals(true, new BinaryLogicProcessor(TRUE, TRUE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
    }

    public void testAndNullHandling() {
        assertNull(new BinaryLogicProcessor(TRUE, NULL, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertNull(new BinaryLogicProcessor(NULL, TRUE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertEquals(false, new BinaryLogicProcessor(FALSE, NULL, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertEquals(false, new BinaryLogicProcessor(NULL, FALSE, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
        assertNull(new BinaryLogicProcessor(NULL, NULL, BinaryLogicProcessor.BinaryLogicOperation.AND).process(null));
    }
}
