/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;

public class ConstantProcessorTests extends AbstractWireSerializingTestCase<ConstantProcessor> {

    public static ConstantProcessor randomConstantProcessor() {
        if (randomBoolean()) {
            Clock clock = Clock.tickMillis(randomZone());
            if (randomBoolean()) {
                clock = Clock.tick(clock, Duration.ofNanos(1));
            }
            return new ConstantProcessor(ZonedDateTime.now(clock));
        } else {
            return new ConstantProcessor(randomAlphaOfLength(5));
        }
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
    protected ConstantProcessor mutateInstance(ConstantProcessor instance) {
        return new ConstantProcessor(randomValueOtherThan(instance.process(null), () -> randomLong()));
    }

    public void testApply() {
        ConstantProcessor proc = new ConstantProcessor("test");
        assertEquals("test", proc.process(null));
        assertEquals("test", proc.process("cat"));
    }

    public void testReadWriteVersion() throws IOException {
        ConstantProcessor original = new ConstantProcessor(new Version("1.2.3"));
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); StreamOutput out = new OutputStreamStreamOutput(baos)) {
            original.writeTo(out);
            try (StreamInput is = new ByteArrayStreamInput(baos.toByteArray())) {
                ConstantProcessor result = new ConstantProcessor(is);
                assertEquals(Version.class, result.process(null).getClass());
                assertEquals("1.2.3", result.process(null).toString());
            }
        }
    }
}
