/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.elasticsearch.test.ESTestCase;

import java.lang.invoke.MethodType;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class EvaluatorKernelTests extends ESTestCase {
    /**
     * Stands in for a generated evaluator: a public class with a {@code public static process} method whose
     * signature matches its kernel descriptor.
     */
    public static final class Shaped {
        public static long process(long a, int b, double scale) {
            return Math.addExact(a, b) * (long) scale;
        }
    }

    /**
     * Stands in for a generated evaluator whose descriptor drifted away from its forwarder.
     */
    public static final class Drifted {
        public static long process(long a) {
            return a;
        }
    }

    private static final EvaluatorKernel SHAPED = new EvaluatorKernel(
        Shaped.class,
        long.class,
        List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.value(int.class), EvaluatorKernel.fixed(double.class)),
        List.of(ArithmeticException.class)
    );

    public void testMethodType() {
        assertThat(SHAPED.methodType(), equalTo(MethodType.methodType(long.class, long.class, int.class, double.class)));
    }

    public void testValueParameterCount() {
        assertThat(SHAPED.valueParameterCount(), equalTo(2));
    }

    public void testMethodHandleCallsForwarder() throws Throwable {
        long a = randomLongBetween(-1_000_000, 1_000_000);
        int b = randomIntBetween(-1_000_000, 1_000_000);
        assertThat((long) SHAPED.methodHandle().invokeExact(a, b, 3.0), equalTo(Shaped.process(a, b, 3.0)));
    }

    public void testMethodHandlePropagatesWarnException() {
        // invokeExact is signature polymorphic: the cast is what makes the call site's type match the kernel's
        expectThrows(ArithmeticException.class, () -> { long ignored = (long) SHAPED.methodHandle().invokeExact(Long.MAX_VALUE, 1, 1.0); });
    }

    public void testDriftedDescriptorIsRejected() {
        EvaluatorKernel drifted = new EvaluatorKernel(
            Drifted.class,
            long.class,
            List.of(EvaluatorKernel.value(long.class), EvaluatorKernel.value(long.class)),
            List.of()
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, drifted::methodHandle);
        assertThat(e.getMessage(), containsString(EvaluatorKernel.METHOD_NAME));
    }
}
