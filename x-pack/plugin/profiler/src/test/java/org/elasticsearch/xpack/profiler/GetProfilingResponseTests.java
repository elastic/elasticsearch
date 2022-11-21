/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public class GetProfilingResponseTests extends ESTestCase {
    private <T> T randomNullable(Supplier<T> v) {
        return randomBoolean() ? v.get() : null;
    }

    private <T> T randomNullable(T v) {
        return randomBoolean() ? v : null;
    }

    public void testSerialization() throws IOException {
        int totalFrames = randomIntBetween(1, 100);

        Map<String, StackTrace> stackTraces = randomNullable(
            Map.of(
                "QjoLteG7HX3VUUXr-J4kHQ",
                new StackTrace(
                    new int[] { 1083999 },
                    new String[] { "QCCDqjSg3bMK1C4YRK6Tiw" },
                    new String[] { "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf" },
                    new int[] { 2 }
                )
            )
        );
        Map<String, StackFrame> stackFrames = randomNullable(
            Map.of(
                "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf",
                new StackFrame(
                    randomNullable(() -> randomAlphaOfLength(20)),
                    randomNullable(() -> randomAlphaOfLength(20)),
                    randomNullable(() -> randomIntBetween(1, Integer.MAX_VALUE)),
                    randomNullable(() -> randomIntBetween(1, 30_000)),
                    randomNullable(() -> randomIntBetween(1, 10))
                )
            )
        );
        Map<String, String> executables = randomNullable(Map.of("QCCDqjSg3bMK1C4YRK6Tiw", "libc.so.6"));
        Map<String, Integer> stackTraceEvents = randomNullable(Map.of(randomAlphaOfLength(12), randomIntBetween(1, 200)));

        GetProfilingResponse request = new GetProfilingResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                GetProfilingResponse deserialized = new GetProfilingResponse(in);
                assertEquals(stackTraces, deserialized.getStackTraces());
                assertEquals(stackFrames, deserialized.getStackFrames());
                assertEquals(executables, deserialized.getExecutables());
                assertEquals(stackTraceEvents, deserialized.getStackTraceEvents());
                assertEquals(totalFrames, deserialized.getTotalFrames());
            }
        }
    }
}
