/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class GetStackTracesResponseTests extends ESTestCase {
    private <T> T randomNullable(T v) {
        return randomBoolean() ? v : null;
    }

    private GetStackTracesResponse createTestInstance() {
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
                    randomList(0, maxInlined, () -> randomIntBetween(1, 30_000))
                )
            )
        );
        Map<String, String> executables = randomNullable(Map.of("QCCDqjSg3bMK1C4YRK6Tiw", "libc.so.6"));
        long totalSamples = randomLongBetween(1L, 200L);
        String stackTraceID = randomAlphaOfLength(12);
        Map<TraceEventID, TraceEvent> stackTraceEvents = randomNullable(
            Map.of(
                new TraceEventID("", "", "", stackTraceID, TransportGetStackTracesAction.DEFAULT_SAMPLING_FREQUENCY),
                new TraceEvent(totalSamples)
            )
        );

        return new GetStackTracesResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, 1.0, totalSamples);
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), instance -> {
            // start and {sampling_rate; end}; see GetStackTracesResponse.toXContentChunked()
            int chunks = 2;
            chunks += size(instance.getExecutables());
            chunks += size(instance.getStackFrames());
            chunks += size(instance.getStackTraces());
            chunks += size(instance.getStackTraceEvents());
            return chunks;
        });
    }

    private int size(Map<?, ?> m) {
        // if there is a map, we also need to take into account start and end object
        return m != null ? 2 + m.size() : 0;
    }
}
