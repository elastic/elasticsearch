/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests that the backwards-compatibility branch in {@link BatchExchangeStatusResponse}
 * properly <em>takes</em> warnings out of the {@link ThreadContext} rather than just
 * <em>reading</em> them. Without the take, warnings end up in both the structured
 * {@link BatchExchangeStatusResponse#warnings()} field and the thread context response
 * headers, leading to duplicated warnings in the final response.
 */
public class BatchExchangeStatusResponseBwcWarningsTests extends ESTestCase {

    /**
     * Simulates receiving a {@link BatchExchangeStatusResponse} from an old node that
     * doesn't support the ESQL_DRIVER_WARNINGS wire field. Warnings travel as transport
     * response headers and are deposited into the thread context before deserialization.
     * After the first read eats them, a second read should get nothing.
     */
    public void testBwcReadTakesWarningsFromThreadContext() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        // Serialize a success response at the old version (won't write warnings field)
        BatchExchangeStatusResponse original = new BatchExchangeStatusResponse(123L, List.of("warn1", "warn2"));
        BytesReference bytes = serialize(original, oldVersion);

        // Simulate transport depositing warnings into thread context
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("lookup field [x] has multiple values"));
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("lookup field [y] is deprecated"));

        // First deserialization should eat the warnings from thread context
        BatchExchangeStatusResponse first = deserialize(bytes, oldVersion, threadContext);
        assertThat(first.warnings(), hasSize(2));
        assertThat(first.warnings(), contains("lookup field [x] has multiple values", "lookup field [y] is deprecated"));

        // A second deserialization on the same thread context should get no warnings
        BatchExchangeStatusResponse second = deserialize(bytes, oldVersion, threadContext);
        assertThat(
            "first read must eat warnings so a second read on the same thread context does not duplicate them",
            second.warnings(),
            empty()
        );
    }

    /**
     * When there are no warnings in the thread context and the version is old,
     * the deserialized warnings should be empty.
     */
    public void testBwcReadWithNoWarnings() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(DriverCompletionInfo.ESQL_DRIVER_WARNINGS);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        BatchExchangeStatusResponse original = new BatchExchangeStatusResponse(0L, List.of());
        BytesReference bytes = serialize(original, oldVersion);

        BatchExchangeStatusResponse deserialized = deserialize(bytes, oldVersion, threadContext);

        assertThat(deserialized.warnings(), empty());
        assertThat(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()), empty());
    }

    private static BytesReference serialize(BatchExchangeStatusResponse response, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            response.writeTo(out);
            return out.bytes();
        }
    }

    private static BatchExchangeStatusResponse deserialize(BytesReference bytes, TransportVersion version, ThreadContext threadContext)
        throws IOException {
        try (StreamInput in = bytes.streamInput()) {
            in.setTransportVersion(version);
            return new BatchExchangeStatusResponse(in, threadContext);
        }
    }
}
