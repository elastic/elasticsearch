/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.Map;

public class GetStackTracesResponseTests extends AbstractWireSerializingTestCase<GetStackTracesResponse> {
    private <T> T randomNullable(T v) {
        return randomBoolean() ? v : null;
    }

    @Override
    protected GetStackTracesResponse createTestInstance() {
        int totalFrames = randomIntBetween(1, 100);

        Map<String, StackTrace> stackTraces = randomNullable(
            Map.of(
                "QjoLteG7HX3VUUXr-J4kHQ",
                new StackTrace(
                    List.of(1083999),
                    List.of("QCCDqjSg3bMK1C4YRK6Tiw"),
                    List.of("QCCDqjSg3bMK1C4YRK6TiwAAAAAAEIpf"),
                    List.of(2),
                    0.3d,
                    2.7d,
                    1
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
        Map<String, TraceEvent> stackTraceEvents = randomNullable(Map.of(stackTraceID, new TraceEvent(stackTraceID, totalSamples)));

        return new GetStackTracesResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, 1.0, totalSamples);
    }

    @Override
    protected GetStackTracesResponse mutateInstance(GetStackTracesResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<GetStackTracesResponse> instanceReader() {
        return GetStackTracesResponse::new;
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), instance -> {
            // start, end, total_frames, samplingrate
            int chunks = 4;
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
