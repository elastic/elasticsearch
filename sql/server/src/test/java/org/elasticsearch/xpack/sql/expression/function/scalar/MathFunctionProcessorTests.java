/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;

import java.io.IOException;

public class MathFunctionProcessorTests extends AbstractWireSerializingTestCase<MathFunctionProcessor> {
    public static MathFunctionProcessor randomMathFunctionProcessor() {
        return new MathFunctionProcessor(randomFrom(MathProcessor.values()));
    }

    @Override
    protected MathFunctionProcessor createTestInstance() {
        return randomMathFunctionProcessor();
    }

    @Override
    protected Reader<MathFunctionProcessor> instanceReader() {
        return MathFunctionProcessor::new;
    }

    @Override
    protected MathFunctionProcessor mutateInstance(MathFunctionProcessor instance) throws IOException {
        return new MathFunctionProcessor(randomValueOtherThan(instance.processor(), () -> randomFrom(MathProcessor.values())));
    }

    public void testApply() {
        MathFunctionProcessor proc = new MathFunctionProcessor(MathProcessor.E);
        assertEquals(Math.E, proc.apply(null));
        assertEquals(Math.E, proc.apply("cat"));
        assertEquals(Math.E, proc.apply(Math.PI));

        proc = new MathFunctionProcessor(MathProcessor.SQRT);
        assertEquals(2.0, (double) proc.apply(4), 0);
        assertEquals(3.0, (double) proc.apply(9d), 0);
        assertEquals(1.77, (double) proc.apply(3.14), 0.01);
    }
}
