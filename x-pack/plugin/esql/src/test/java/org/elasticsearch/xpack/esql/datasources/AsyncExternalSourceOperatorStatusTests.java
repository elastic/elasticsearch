/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class AsyncExternalSourceOperatorStatusTests extends AbstractWireSerializingTestCase<AsyncExternalSourceOperator.Status> {

    @Override
    protected Writeable.Reader<AsyncExternalSourceOperator.Status> instanceReader() {
        return AsyncExternalSourceOperator.Status::new;
    }

    @Override
    protected AsyncExternalSourceOperator.Status createTestInstance() {
        return new AsyncExternalSourceOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            null,
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomFormatReader()
        );
    }

    private static Map<String, Object> randomFormatReader() {
        return switch (between(0, 3)) {
            case 0 -> Map.of();
            case 1 -> Map.of("row_groups_read", randomNonNegativeLong());
            case 2 -> Map.of("row_groups_read", randomNonNegativeLong(), "rows_filtered", randomNonNegativeLong());
            // Case 3 exercises the producer-realistic shape: a "columns" key carrying nested
            // Map<String, Map<String, Object>> per-column entries — the same shape ParquetReaderCounters
            // emits today. Without this case, the wire round-trip in AbstractWireSerializingTestCase
            // never sees the nested map and a PerColumnStatus regression would not be caught here.
            case 3 -> Map.of(
                "row_groups_kept",
                randomNonNegativeLong(),
                "total_read_nanos",
                randomNonNegativeLong(),
                "columns",
                Map.of(
                    "host",
                    Map.of(
                        "bytes_compressed_read",
                        randomNonNegativeLong(),
                        "decode_nanos",
                        randomNonNegativeLong(),
                        "materialization",
                        randomFrom("eager", "late")
                    ),
                    "status_code",
                    Map.of("pages_read", randomNonNegativeLong(), "materialization", "eager")
                )
            );
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    protected AsyncExternalSourceOperator.Status mutateInstance(AsyncExternalSourceOperator.Status instance) throws IOException {
        int pagesWaiting = instance.pagesWaiting();
        int pagesEmitted = instance.pagesEmitted();
        long rowsEmitted = instance.rowsEmitted();
        long bytesBuffered = instance.bytesBuffered();
        long processNanos = instance.processNanos();
        int splitsProcessed = instance.splitsProcessed();
        int splitsTotal = instance.splitsTotal();
        int currentSplit = instance.currentSplit();
        long bytesRead = instance.bytesRead();
        Map<String, Object> formatReader = instance.formatReader();
        switch (between(0, 9)) {
            case 0 -> pagesWaiting = randomValueOtherThan(pagesWaiting, ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 3 -> bytesBuffered = randomValueOtherThan(bytesBuffered, ESTestCase::randomNonNegativeLong);
            case 4 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 5 -> splitsProcessed = randomValueOtherThan(splitsProcessed, ESTestCase::randomNonNegativeInt);
            case 6 -> splitsTotal = randomValueOtherThan(splitsTotal, ESTestCase::randomNonNegativeInt);
            case 7 -> currentSplit = randomValueOtherThan(currentSplit, ESTestCase::randomNonNegativeInt);
            case 8 -> bytesRead = randomValueOtherThan(bytesRead, ESTestCase::randomNonNegativeLong);
            case 9 -> formatReader = randomValueOtherThan(formatReader, AsyncExternalSourceOperatorStatusTests::randomFormatReader);
            default -> throw new UnsupportedOperationException();
        }
        return new AsyncExternalSourceOperator.Status(
            pagesWaiting,
            pagesEmitted,
            rowsEmitted,
            bytesBuffered,
            null,
            processNanos,
            splitsProcessed,
            splitsTotal,
            currentSplit,
            bytesRead,
            0L,
            formatReader
        );
    }

    public void testToXContent() {
        assertThat(
            Strings.toString(
                new AsyncExternalSourceOperator.Status(5, 10, 111, 2048, null, 1_000_000L, 2, 4, 3, 8192L, 0L, Map.of("k", 7L))
            ),
            equalTo(
                "{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048,"
                    + "\"process_nanos\":1000000,\"splits_processed\":2,\"splits_total\":4,\"current_split\":3,"
                    + "\"bytes_read\":8192,\"read_nanos\":0,\"format_reader\":{\"k\":7}}"
            )
        );
    }

    public void testToXContentWithFailure() {
        assertThat(
            Strings.toString(
                new AsyncExternalSourceOperator.Status(5, 10, 111, 2048, new RuntimeException("boom"), 0L, 0, 0, 0, 0L, 0L, Map.of())
            ),
            equalTo(
                "{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048,"
                    + "\"process_nanos\":0,\"splits_processed\":0,\"splits_total\":0,\"current_split\":0,"
                    + "\"bytes_read\":0,\"read_nanos\":0,\"format_reader\":{},\"failure\":\"boom\"}"
            )
        );
    }

    public void testReadFromBwcVersionPriorToTelemetry() throws IOException {
        AsyncExternalSourceOperator.Status original = new AsyncExternalSourceOperator.Status(
            5,
            10,
            111,
            2048,
            null,
            1_000_000L,
            2,
            4,
            3,
            8192L,
            0L,
            Map.of("row_groups_read", 7L)
        );
        TransportVersion preTelemetry = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("esql_external_source_telemetry")
        );
        AsyncExternalSourceOperator.Status copy = copyInstance(original, preTelemetry);
        // Pre-telemetry fields round-trip
        assertThat(copy.pagesWaiting(), equalTo(5));
        assertThat(copy.pagesEmitted(), equalTo(10));
        assertThat(copy.rowsEmitted(), equalTo(111L));
        assertThat(copy.bytesBuffered(), equalTo(2048L));
        // Telemetry fields default to zero/empty on the receiving end
        assertThat(copy.processNanos(), equalTo(0L));
        assertThat(copy.splitsProcessed(), equalTo(0));
        assertThat(copy.splitsTotal(), equalTo(0));
        assertThat(copy.currentSplit(), equalTo(0));
        assertThat(copy.bytesRead(), equalTo(0L));
        assertThat(copy.formatReader(), equalTo(Map.of()));
    }

    /**
     * Locks down the wire format of the {@code formatReader} carrier with a producer-realistic
     * payload: scalar top-level keys plus a {@code columns} key whose value is a nested
     * {@code Map<String, Map<String, Object>>}. {@code StreamOutput.writeGenericMap} only handles
     * leaf types in its {@code WRITERS} registry; routing a typed record (e.g. {@code PerColumnStatus})
     * directly through this path throws at runtime and silently breaks production reads. Keeping the
     * nested-Map payload in the random-instance space catches that regression in the round-trip.
     */
    public void testFormatReaderRoundTripWithNestedColumnsMap() throws IOException {
        Map<String, Object> formatReader = Map.of(
            "row_groups_in_file",
            42L,
            "row_groups_kept",
            7L,
            "total_read_nanos",
            123_456L,
            "columns",
            Map.of(
                "host",
                Map.of("bytes_compressed_read", 1024L, "decode_nanos", 500L, "materialization", "late"),
                "status_code",
                Map.of("pages_read", 9L, "materialization", "eager")
            )
        );
        AsyncExternalSourceOperator.Status original = new AsyncExternalSourceOperator.Status(
            1,
            2,
            3L,
            4L,
            null,
            5L,
            6,
            7,
            8,
            9L,
            10L,
            formatReader
        );
        AsyncExternalSourceOperator.Status copy = copyInstance(original);
        assertThat(copy.formatReader(), equalTo(formatReader));
    }
}
