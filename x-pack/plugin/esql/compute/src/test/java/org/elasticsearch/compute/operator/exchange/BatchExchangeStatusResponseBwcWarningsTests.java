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
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

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

    public void testProfileRoundTrip() throws IOException {
        BatchExchangeStatusResponse.Profile profile = new BatchExchangeStatusResponse.Profile(
            101L,
            102L,
            103L,
            104L,
            105L,
            106L,
            107L,
            108L,
            109L,
            110L,
            111L
        );
        BatchExchangeStatusResponse original = new BatchExchangeStatusResponse(108L, List.of("warning"), profile);

        BytesReference bytes = serialize(original, TransportVersion.current());
        BatchExchangeStatusResponse deserialized = deserialize(bytes, TransportVersion.current(), new ThreadContext(Settings.EMPTY));

        assertThat(deserialized.bytesRead(), equalTo(108L));
        assertThat(deserialized.warnings(), contains("warning"));
        assertThat(deserialized.profile(), equalTo(profile));
    }

    public void testProfileIsOmittedForOldVersions() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE);
        BatchExchangeStatusResponse original = new BatchExchangeStatusResponse(
            108L,
            List.of(),
            new BatchExchangeStatusResponse.Profile(101L, 102L, 103L, 104L, 105L, 106L, 107L)
        );

        BytesReference bytes = serialize(original, oldVersion);
        BatchExchangeStatusResponse deserialized = deserialize(bytes, oldVersion, new ThreadContext(Settings.EMPTY));

        assertThat(deserialized.profile(), nullValue());
    }

    public void testGranularProfileIsOmittedBeforeGranularVersion() throws IOException {
        BatchExchangeStatusResponse.Profile profile = new BatchExchangeStatusResponse.Profile(
            101L,
            102L,
            103L,
            104L,
            105L,
            106L,
            107L,
            108L,
            109L,
            110L,
            111L
        );
        BatchExchangeStatusResponse original = new BatchExchangeStatusResponse(108L, List.of(), profile);

        BytesReference bytes = serialize(original, BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE);
        BatchExchangeStatusResponse deserialized = deserialize(
            bytes,
            BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE,
            new ThreadContext(Settings.EMPTY)
        );

        assertThat(deserialized.profile(), equalTo(new BatchExchangeStatusResponse.Profile(101L, 102L, 103L, 104L, 105L, 106L, 107L)));
    }

    public void testProfileSummarizesDriverAndFieldLoading() {
        ValuesSourceReaderOperatorStatus readerStatus = new ValuesSourceReaderOperatorStatus(
            Map.of("DocValuesReader", 1),
            Map.of(),
            104L,
            1,
            1,
            5L,
            5L,
            103L,
            0L,
            105L,
            106L,
            107L
        );
        DriverProfile driverProfile = new DriverProfile(
            "remote fetch",
            "cluster",
            "node",
            1L,
            2L,
            999L,
            102L,
            1L,
            List.of(
                new OperatorStatus("ExchangeSourceOperator[]", new ExchangeSourceOperator.Status(0, 8, 111L)),
                new OperatorStatus("values reader", readerStatus),
                new OperatorStatus("ExchangeSinkOperator", new ExchangeSinkOperator.Status(9, 112L))
            ),
            new DriverSleeps(Map.of(), List.of(), List.of())
        );

        BatchExchangeStatusResponse.Profile profile = BatchExchangeStatusResponse.Profile.from(driverProfile, 101L);

        assertThat(profile.driverTookNanos(), equalTo(101L));
        assertThat(profile.driverCpuNanos(), equalTo(102L));
        assertThat(profile.valuesLoaded(), equalTo(103L));
        assertThat(profile.fieldLoadNanos(), equalTo(104L));
        assertThat(profile.sourceDocsLoaded(), equalTo(105L));
        assertThat(profile.sourceFieldReads(), equalTo(106L));
        assertThat(profile.sourceBytesLoaded(), equalTo(107L));
        assertThat(profile.requestPages(), equalTo(8L));
        assertThat(profile.requestRows(), equalTo(111L));
        assertThat(profile.responsePages(), equalTo(9L));
        assertThat(profile.responseRows(), equalTo(112L));
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
