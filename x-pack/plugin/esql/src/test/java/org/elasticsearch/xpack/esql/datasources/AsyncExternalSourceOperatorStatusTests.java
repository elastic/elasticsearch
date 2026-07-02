/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.datasource.csv.CsvReaderStatus;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonReaderStatus;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AsyncExternalSourceOperatorStatusTests extends AbstractWireSerializingTestCase<AsyncExternalSourceOperator.Status> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        // The typed format_reader payload is a NamedWriteable contributed by each format module; the
        // csv + ndjson modules are on the unit-test classpath, so register their entries here.
        return new NamedWriteableRegistry(List.of(NdJsonReaderStatus.ENTRY, CsvReaderStatus.ENTRY));
    }

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
            randomFormatReader(),
            randomCapturedSourceMetadata(),
            randomBoolean()
        );
    }

    private static Map<String, List<Map<String, Object>>> randomCapturedSourceMetadata() {
        if (randomBoolean()) {
            return Map.of();
        }
        Map<String, List<Map<String, Object>>> out = new HashMap<>();
        int files = between(1, 2);
        for (int f = 0; f < files; f++) {
            List<Map<String, Object>> contributions = new ArrayList<>();
            int n = between(1, 2);
            for (int i = 0; i < n; i++) {
                Map<String, Object> stats = new HashMap<>();
                stats.put("_stats.row_count", randomNonNegativeLong());
                contributions.add(stats);
            }
            out.put("file-" + f + "-" + randomAlphaOfLength(4), contributions);
        }
        return out;
    }

    private static FormatReaderStatus randomFormatReader() {
        return switch (between(0, 2)) {
            case 0 -> null;
            case 1 -> new NdJsonReaderStatus(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            case 2 -> new CsvReaderStatus(
                randomFrom("csv", "tsv"),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomBoolean(),
                randomNonNegativeLong()
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
        FormatReaderStatus formatReader = instance.formatReader();
        Map<String, List<Map<String, Object>>> capturedSourceMetadata = instance.capturedSourceMetadata();
        boolean partial = instance.partial();
        switch (between(0, 11)) {
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
            case 10 -> capturedSourceMetadata = randomValueOtherThan(
                capturedSourceMetadata,
                AsyncExternalSourceOperatorStatusTests::randomCapturedSourceMetadata
            );
            case 11 -> partial = partial == false;
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
            formatReader,
            capturedSourceMetadata,
            partial
        );
    }

    public void testToXContent() {
        assertThat(
            Strings.toString(
                new AsyncExternalSourceOperator.Status(
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
                    new NdJsonReaderStatus(7L, 0L, 0L),
                    Map.of(),
                    false
                )
            ),
            equalTo(
                "{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048,"
                    + "\"process_nanos\":1000000,\"splits_processed\":2,\"splits_total\":4,\"current_split\":3,"
                    + "\"bytes_read\":8192,\"read_nanos\":0,\"partial\":false,"
                    + "\"format_reader\":{\"format\":\"ndjson\",\"rows_emitted\":7,\"parse_errors\":0,\"read_nanos\":0}}"
            )
        );
    }

    public void testToXContentWithFailure() {
        assertThat(
            Strings.toString(
                new AsyncExternalSourceOperator.Status(
                    5,
                    10,
                    111,
                    2048,
                    new RuntimeException("boom"),
                    0L,
                    0,
                    0,
                    0,
                    0L,
                    0L,
                    null,
                    Map.of(),
                    true
                )
            ),
            equalTo(
                "{\"pages_waiting\":5,\"pages_emitted\":10,\"rows_emitted\":111,\"bytes_buffered\":2048,"
                    + "\"process_nanos\":0,\"splits_processed\":0,\"splits_total\":0,\"current_split\":0,"
                    + "\"bytes_read\":0,\"read_nanos\":0,\"partial\":true,\"format_reader\":{},\"failure\":\"boom\"}"
            )
        );
    }

    public void testReadFromBwcVersionPriorToProfile() throws IOException {
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
            new NdJsonReaderStatus(7L, 0L, 0L),
            Map.of(),
            true
        );
        TransportVersion preProfile = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_external_source_profile"));
        AsyncExternalSourceOperator.Status copy = copyInstance(original, preProfile);
        // Pre-profile fields round-trip
        assertThat(copy.pagesWaiting(), equalTo(5));
        assertThat(copy.pagesEmitted(), equalTo(10));
        assertThat(copy.rowsEmitted(), equalTo(111L));
        assertThat(copy.bytesBuffered(), equalTo(2048L));
        // Profile fields default to zero / null on the receiving end
        assertThat(copy.processNanos(), equalTo(0L));
        assertThat(copy.splitsProcessed(), equalTo(0));
        assertThat(copy.splitsTotal(), equalTo(0));
        assertThat(copy.currentSplit(), equalTo(0));
        assertThat(copy.bytesRead(), equalTo(0L));
        assertThat(copy.formatReader(), nullValue());
        // partial is gated by an even newer version, so it also defaults to false on the receiving end
        assertThat(copy.partial(), equalTo(false));
    }

    public void testReadFromBwcVersionPriorToPartial() throws IOException {
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
            new NdJsonReaderStatus(7L, 0L, 0L),
            Map.of(),
            true
        );
        TransportVersion prePartial = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_external_partial_results"));
        AsyncExternalSourceOperator.Status copy = copyInstance(original, prePartial);
        // profile fields still round-trip at this version
        assertThat(copy.bytesRead(), equalTo(8192L));
        // partial was not yet on the wire, so it defaults to false
        assertThat(copy.partial(), equalTo(false));
    }

    public void testTypedFormatReaderRoundTrip() throws IOException {
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
            new CsvReaderStatus("tsv", 42L, 3L, true, 123_456L),
            Map.of(),
            true
        );
        AsyncExternalSourceOperator.Status copy = copyInstance(original);
        assertThat(copy.formatReader(), equalTo(new CsvReaderStatus("tsv", 42L, 3L, true, 123_456L)));
        assertThat(copy.partial(), equalTo(true));
    }
}
