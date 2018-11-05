/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

public class BinaryLogicProcessorTests extends AbstractWireSerializingTestCase<BinaryLogicProcessor> {

    private static final Processor FALSE = new ConstantProcessor(false);
    private static final Processor TRUE = new ConstantProcessor(true);
    private static final Processor NULL = new ConstantProcessor((Object) null);

    public static BinaryLogicProcessor randomProcessor() {
        return new BinaryLogicProcessor(
                new ConstantProcessor(randomBooleanOrNull()),
                new ConstantProcessor(randomBoolean()),
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

    private static Boolean randomBooleanOrNull() {
        int i = randomIntBetween(0, 2);
        switch (i) {
            case 0:
                return null;
            case 1:
                return Boolean.FALSE;
            case 2:
                return Boolean.TRUE;
            default:
                return Boolean.FALSE;
        }
    }
}
