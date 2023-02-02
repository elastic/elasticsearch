/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;
import java.util.function.Supplier;

public class GetProfilingResponseTests extends AbstractWireSerializingTestCase<GetProfilingResponse> {
    private <T> T randomNullable(Supplier<T> v) {
        return randomBoolean() ? v.get() : null;
    }

    private <T> T randomNullable(T v) {
        return randomBoolean() ? v : null;
    }

    @Override
    protected GetProfilingResponse createTestInstance() {
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
        int maxInlined = randomInt(5);
        Map<String, StackFrame> stackFrames = randomNullable(
            Map.of(
                "QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf",
                new StackFrame(
                    randomList(0, maxInlined, () -> randomAlphaOfLength(20)),
                    randomList(0, maxInlined, () -> randomAlphaOfLength(20)),
                    randomList(0, maxInlined, () -> randomIntBetween(1, Integer.MAX_VALUE)),
                    randomList(0, maxInlined, () -> randomIntBetween(1, 30_000)),
                    randomList(0, maxInlined, () -> randomIntBetween(1, 10))
                )
            )
        );
        Map<String, String> executables = randomNullable(Map.of("QCCDqjSg3bMK1C4YRK6Tiw", "libc.so.6"));
        Map<String, Integer> stackTraceEvents = randomNullable(Map.of(randomAlphaOfLength(12), randomIntBetween(1, 200)));

        return new GetProfilingResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames);
    }

    @Override
    protected GetProfilingResponse mutateInstance(GetProfilingResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<GetProfilingResponse> instanceReader() {
        return GetProfilingResponse::new;
    }
}
