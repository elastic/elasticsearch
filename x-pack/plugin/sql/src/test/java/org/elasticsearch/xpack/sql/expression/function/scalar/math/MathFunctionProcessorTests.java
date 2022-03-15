/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

public class MathFunctionProcessorTests extends AbstractWireSerializingTestCase<MathProcessor> {
    public static MathProcessor randomMathFunctionProcessor() {
        return new MathProcessor(randomFrom(MathOperation.values()));
    }

    @Override
    protected MathProcessor createTestInstance() {
        return randomMathFunctionProcessor();
    }

    @Override
    protected Reader<MathProcessor> instanceReader() {
        return MathProcessor::new;
    }

    @Override
    protected MathProcessor mutateInstance(MathProcessor instance) throws IOException {
        return new MathProcessor(randomValueOtherThan(instance.processor(), () -> randomFrom(MathOperation.values())));
    }

    public void testApply() {
        MathProcessor proc = new MathProcessor(MathOperation.E);
        assertEquals(Math.E, proc.process(null));
        assertEquals(Math.E, proc.process(Math.PI));

        proc = new MathProcessor(MathOperation.SQRT);
        assertEquals(2.0, (double) proc.process(4), 0);
        assertEquals(3.0, (double) proc.process(9d), 0);
        assertEquals(1.77, (double) proc.process(3.14), 0.01);
    }

    public void testNumberCheck() {
        MathProcessor proc = new MathProcessor(MathOperation.E);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("string"));
        assertEquals("A number is required; received [string]", siae.getMessage());

    }

    public void testRandom() {
        MathProcessor proc = new MathProcessor(MathOperation.RANDOM);
        assertNull(proc.process(null));
        assertNotNull(proc.process(randomLong()));
    }

    public void testFloor() {
        MathProcessor proc = new MathProcessor(MathOperation.FLOOR);
        assertNull(proc.process(null));
        assertNotNull(proc.process(randomLong()));
        assertEquals(3.0, proc.process(3.3));
        assertEquals(3.0, proc.process(3.9));
        assertEquals(-13.0, proc.process(-12.1));
    }

    public void testCeil() {
        MathProcessor proc = new MathProcessor(MathOperation.CEIL);
        assertNull(proc.process(null));
        assertNotNull(proc.process(randomLong()));
        assertEquals(4.0, proc.process(3.3));
        assertEquals(4.0, proc.process(3.9));
        assertEquals(-12.0, proc.process(-12.1));
    }

    public void testUnsignedLongAbs() {
        MathProcessor proc = new MathProcessor(MathOperation.ABS);
        BigInteger bi = randomBigInteger();
        assertEquals(bi, proc.process(bi));
    }

    public void testUnsignedLongSign() {
        MathProcessor proc = new MathProcessor(MathOperation.SIGN);
        for (BigInteger bi : Arrays.asList(BigInteger.valueOf(randomNonNegativeLong()), BigInteger.ZERO)) {
            Object val = proc.process(bi);
            assertEquals(bi.intValue() == 0 ? 0 : 1, val);
            assertTrue(val instanceof Integer);
        }
    }
}
