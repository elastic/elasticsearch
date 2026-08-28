/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests that the backwards-compatibility branch in {@link DriverCompletionInfo#readFrom}
 * properly <em>takes</em> warnings out of the {@link ThreadContext} rather than just
 * <em>reading</em> them. Without the take, warnings end up in both the structured
 * {@link DriverCompletionInfo#warnings()} field and the thread context response headers,
 * leading to duplicated warnings in the final response.
 */
public class DriverCompletionInfoBwcWarningsTests extends ESTestCase {

    /**
     * Simulates receiving a {@link DriverCompletionInfo} from an old node that doesn't
     * support {@link DriverCompletionInfo#ESQL_DRIVER_WARNINGS}. Warnings travel as
     * transport response headers and are deposited into the thread context before
     * deserialization. After reading, the first deserialized result should have the warnings
     * and a second deserialization from the same thread context should get nothing.
     */
    public void testBwcReadTakesWarningsFromThreadContext() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        // Serialize a DriverCompletionInfo at the old version (won't write warnings field)
        DriverCompletionInfo original = new DriverCompletionInfo(42, 100, 0, 0, 0, 0, 0, List.of(), List.of(), null, false, Set.of());
        BytesReference bytes = serialize(original, oldVersion);

        // Simulate transport depositing warnings into thread context (as the old node would)
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("field [foo] is deprecated"));
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("field [bar] is deprecated"));

        // Verify warnings are in thread context before deserialization
        List<String> headersBefore = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        assertThat(headersBefore, hasSize(2));

        // First deserialization should eat the warnings from thread context
        DriverCompletionInfo first = deserialize(bytes, oldVersion, threadContext);
        assertThat(first.warnings(), hasSize(2));
        assertThat(first.warnings(), contains("field [foo] is deprecated", "field [bar] is deprecated"));

        // A second deserialization on the same thread context should get no warnings —
        // the first call should have eaten them. Without takeResponseHeaders this second
        // call would also see the same warnings, duplicating them.
        DriverCompletionInfo second = deserialize(bytes, oldVersion, threadContext);
        assertThat(
            "first readFrom must eat warnings so a second readFrom on the same thread context does not duplicate them",
            second.warnings(),
            empty()
        );
    }

    /**
     * When there are no warnings in the thread context and the version is old,
     * the deserialized warnings should be empty and the thread context should
     * remain clean.
     */
    public void testBwcReadWithNoWarnings() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        DriverCompletionInfo original = new DriverCompletionInfo(1, 2, 0, 0, 0, 0, 0, List.of(), List.of(), null, false, Set.of());
        BytesReference bytes = serialize(original, oldVersion);

        DriverCompletionInfo deserialized = deserialize(bytes, oldVersion, threadContext);

        assertThat(deserialized.warnings(), equalTo(Set.of()));
        assertThat(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()), empty());
    }

    /**
     * When reading from a new version (supports ESQL_DRIVER_WARNINGS), warnings come
     * from the wire, not from the thread context. Any warnings in the thread context
     * should remain untouched — they belong to a different scope.
     */
    public void testNewVersionReadsWarningsFromWire() throws IOException {
        TransportVersion newVersion = TransportVersion.current();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        // Serialize with warnings on the wire
        DriverCompletionInfo original = new DriverCompletionInfo(
            1,
            2,
            0,
            0,
            0,
            0,
            0,
            List.of(),
            List.of(),
            null,
            false,
            Set.of("wire warning 1", "wire warning 2")
        );
        BytesReference bytes = serialize(original, newVersion);

        // Add unrelated warnings to thread context — they should NOT be consumed
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("unrelated thread context warning"));

        DriverCompletionInfo deserialized = deserialize(bytes, newVersion, threadContext);

        // Warnings from the wire (Set has no guaranteed order)
        assertThat(deserialized.warnings(), hasSize(2));
        assertThat(deserialized.warnings(), containsInAnyOrder("wire warning 1", "wire warning 2"));

        // Thread context warnings should be untouched
        List<String> headers = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        assertThat(headers, hasSize(1));
    }

    private static BytesReference serialize(DriverCompletionInfo info, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            info.writeTo(out);
            return out.bytes();
        }
    }

    private static DriverCompletionInfo deserialize(BytesReference bytes, TransportVersion version, ThreadContext threadContext)
        throws IOException {
        try (StreamInput in = bytes.streamInput()) {
            in.setTransportVersion(version);
            return DriverCompletionInfo.readFrom(in, threadContext);
        }
    }
}
