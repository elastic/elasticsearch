/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ConstantProcessorTests extends AbstractWireSerializingTestCase<ConstantProcessor> {
    public static ConstantProcessor randomConstantProcessor() {
        return new ConstantProcessor(randomAlphaOfLength(5));
    }

    @Override
    protected ConstantProcessor createTestInstance() {
        return randomConstantProcessor();
    }

    @Override
    protected Reader<ConstantProcessor> instanceReader() {
        return ConstantProcessor::new;
    }

    @Override
    protected ConstantProcessor mutateInstance(ConstantProcessor instance) throws IOException {
        return new ConstantProcessor(randomValueOtherThan(instance.process(null),
                () -> new IntervalDayTime(Duration.ofSeconds(
                        randomLongBetween(TimeUnit.SECONDS.convert(3, TimeUnit.HOURS), TimeUnit.SECONDS.convert(23, TimeUnit.HOURS))),
                        DataType.INTERVAL_DAY_TO_SECOND)));
    }

    public void testApply() {
        ConstantProcessor proc = new ConstantProcessor("test");
        assertEquals("test", proc.process(null));
        assertEquals("test", proc.process("cat"));
    }
}
