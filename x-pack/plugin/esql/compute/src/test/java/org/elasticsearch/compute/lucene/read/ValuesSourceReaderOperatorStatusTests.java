/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Measurement;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TimerMeasurement;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.profile.CodecReads;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class ValuesSourceReaderOperatorStatusTests extends AbstractWireSerializingTestCase<ValuesSourceReaderOperatorStatus> {
    public static ValuesSourceReaderOperatorStatus simple() {
        return new ValuesSourceReaderOperatorStatus(
            Map.of("ReaderType", 3),
            Map.of(),
            1022323,
            123,
            200,
            111,
            222,
            1000,
            8192,
            16384,
            100,
            300,
            4096,
            CodecReads.EMPTY
        );
    }

    public static String simpleToJson() {
        return """
            {
              "readers_built" : {
                "ReaderType" : 3
              },
              "values_loaded" : 1000,
              "bytes_read" : 8192,
              "bytes_decoded" : 16384,
              "source_docs_loaded" : 100,
              "source_field_reads" : 300,
              "source_bytes_loaded" : 4096,
              "process_nanos" : 1022323,
              "process_time" : "1ms",
              "pages_received" : 123,
              "pages_emitted" : 200,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    public void testNoCodecObjectIsRenderedWhenThereAreNoReadMetrics() {
        final Map<String, Object> rendered = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            Strings.toString(simple(), true, true),
            true
        );
        assertFalse(rendered.containsKey("totals"));
    }

    public void testToXContentWithReadMetrics() {
        final CodecReads reads = new CodecReads(
            Map.of(
                "ColumNAR",
                Snapshot.of(
                    List.of(
                        count("ColumNAR.blocks.fetched", 110),
                        timer("ColumNAR.blocks.fetch_time_nanos", 84211, TimingPolicy.ADAPTIVE, 12, 40),
                        count("ColumNAR.readers.opened", 2),
                        timer("ColumNAR.readers.open_time_nanos", 740750, TimingPolicy.EXACT, 2, 2)
                    )
                )
            ),
            List.of(
                new CodecReads.Detail(
                    ".ds-logs-a:0/_1",
                    "agent.version",
                    "ColumNAR",
                    null,
                    Map.of("skip_index", CodecReads.Fact.TRUE),
                    Snapshot.of(List.of(count("ColumNAR.blocks.fetched", 12), count("ColumNAR.readers.opened", 1)))
                )
            ),
            Map.of(),
            false,
            true,
            true
        );
        final ValuesSourceReaderOperatorStatus status = new ValuesSourceReaderOperatorStatus(
            Map.of("ReaderType", 3),
            Map.of(),
            1022323,
            123,
            200,
            111,
            222,
            1000,
            8192,
            16384,
            100,
            300,
            4096,
            reads
        );
        assertThat(Strings.toString(status, true, true), equalTo("""
            {
              "readers_built" : {
                "ReaderType" : 3
              },
              "values_loaded" : 1000,
              "bytes_read" : 8192,
              "bytes_decoded" : 16384,
              "source_docs_loaded" : 100,
              "source_field_reads" : 300,
              "source_bytes_loaded" : 4096,
              "totals" : {
                "ColumNAR" : {
                  "blocks" : {
                    "fetch_time_nanos" : {
                      "value" : 84211,
                      "unit" : "ns",
                      "estimated" : true,
                      "total_calls" : 40,
                      "sampled_calls" : 12
                    },
                    "fetched" : 110
                  },
                  "readers" : {
                    "open_time_nanos" : {
                      "value" : 740750,
                      "unit" : "ns",
                      "estimated" : false,
                      "total_calls" : 2
                    },
                    "opened" : 2
                  }
                }
              },
              "segments" : {
                ".ds-logs-a:0/_1" : {
                  "metrics" : {
                    "ColumNAR" : {
                      "blocks" : {
                        "fetched" : 12
                      },
                      "readers" : {
                        "opened" : 1
                      }
                    }
                  },
                  "fields" : {
                    "agent.version" : {
                      "column" : {
                        "skip_index" : true
                      },
                      "metrics" : {
                        "ColumNAR" : {
                          "blocks" : {
                            "fetched" : 12
                          },
                          "readers" : {
                            "opened" : 1
                          }
                        }
                      }
                    }
                  }
                }
              },
              "process_nanos" : 1022323,
              "process_time" : "1ms",
              "pages_received" : 123,
              "pages_emitted" : 200,
              "rows_received" : 111,
              "rows_emitted" : 222
            }"""));
    }

    public void testReadMetricsAreDroppedForAReceiverThatDoesNotKnowThem() throws IOException {
        final ValuesSourceReaderOperatorStatus status = createTestInstance();
        final TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("codec_read_profiling"));
        final ValuesSourceReaderOperatorStatus received = copyInstance(status, before);
        assertTrue(received.codecReads().isEmpty());
        assertThat(received.valuesLoaded(), equalTo(status.valuesLoaded()));
        assertThat(received.bytesRead(), equalTo(status.bytesRead()));
        assertThat(received.bytesDecoded(), equalTo(status.bytesDecoded()));
        assertThat(received.readersBuilt(), equalTo(status.readersBuilt()));
    }

    @SuppressWarnings("unchecked")
    public void testTheCodecsAndSegmentsSitBesideTheOldFields() throws IOException {
        for (int round = 0; round < 20; round++) {
            final ValuesSourceReaderOperatorStatus status = createTestInstance();
            final Map<String, Object> rendered = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                Strings.toString(status, true, true),
                true
            );
            assertTrue(rendered.containsKey("bytes_read"));
            assertTrue(rendered.containsKey("values_loaded"));
            assertTrue(rendered.containsKey("readers_built"));
            assertThat(rendered.containsKey("totals"), equalTo(status.codecReads().isEmpty() == false));
            assertThat(
                rendered.containsKey("segments"),
                equalTo(status.codecReads().isEmpty() == false && status.codecReads().details().isEmpty() == false)
            );
            if (rendered.containsKey("totals")) {
                assertThat(
                    "one entry per codec, ordered by name",
                    new ArrayList<>(((Map<String, Object>) rendered.get("totals")).keySet()),
                    equalTo(new ArrayList<>(status.codecReads().totals().keySet()))
                );
            }
        }
    }

    public void testAMetricNameTheSerializerNeverHeardOfSurvivesTheWire() throws IOException {
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", Snapshot.of(List.of(count("Future.new_component.events", 7), count("ColumNAR.chunks.decompressed", 3)))),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final ValuesSourceReaderOperatorStatus status = new ValuesSourceReaderOperatorStatus(
            Map.of(),
            Map.of(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            reads
        );
        final ValuesSourceReaderOperatorStatus received = copyInstance(status);
        assertThat(received.codecReads(), equalTo(reads));
        assertThat(received, equalTo(status));
    }

    public void testTheStatusIsNotAffectedByLaterChangesToTheMapItWasBuiltFrom() {
        final Map<String, Snapshot> live = new TreeMap<>(
            Map.of("ColumNAR", Snapshot.of(List.of(count("ColumNAR.chunks.decompressed", 1))))
        );
        final ValuesSourceReaderOperatorStatus status = new ValuesSourceReaderOperatorStatus(
            Map.of(),
            Map.of(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            new CodecReads(live, List.of(), Map.of(), false, true, true)
        );
        live.put("Other", Snapshot.EMPTY);
        assertThat(status.codecReads().totals().keySet(), equalTo(java.util.Set.of("ColumNAR")));
        expectThrows(UnsupportedOperationException.class, () -> status.codecReads().totals().put("Other", Snapshot.EMPTY));
        expectThrows(
            UnsupportedOperationException.class,
            () -> status.codecReads().totals().get("ColumNAR").measurements().add(count("ColumNAR.a.b", 1))
        );
    }

    private static CounterMeasurement count(String key, long value) {
        final Metric.Id id = Metric.Id.parse(key);
        return new CounterMeasurement(
            CounterMetric.of(id, id.name().contains("bytes") ? Metric.Unit.BYTES : Metric.Unit.COUNT, "d"),
            value
        );
    }

    private static TimerMeasurement timer(String key, long value, TimingPolicy policy, long sampled, long total) {
        return new TimerMeasurement(
            TimerMetric.of(Metric.Id.parse(key), policy, "d"),
            value,
            OptionalLong.empty(),
            sampled,
            total,
            sampled < total
        );
    }

    @Override
    protected Writeable.Reader<ValuesSourceReaderOperatorStatus> instanceReader() {
        return ValuesSourceReaderOperatorStatus::readFrom;
    }

    @Override
    public ValuesSourceReaderOperatorStatus createTestInstance() {
        return new ValuesSourceReaderOperatorStatus(
            randomReadersBuiltOrConvertersUsed(),
            randomReadersBuiltOrConvertersUsed(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomCodecReads()
        );
    }

    private CodecReads randomCodecReads() {
        if (randomBoolean()) {
            return CodecReads.EMPTY;
        }
        final Map<String, Snapshot> totals = new TreeMap<>();
        for (int codec = 0; codec < between(1, 3); codec++) {
            totals.put(randomAlphaOfLength(between(1, 8)), randomMetrics());
        }
        final List<CodecReads.Detail> details = new ArrayList<>();
        for (int pair = 0; pair < between(0, 6); pair++) {
            details.add(
                new CodecReads.Detail(
                    "index:" + between(0, 3) + "/_" + pair,
                    "field_" + pair,
                    totals.keySet().iterator().next(),
                    null,
                    Map.of(),
                    randomMetrics()
                )
            );
        }
        return new CodecReads(totals, details, Map.of(), false, true, true);
    }

    private Snapshot randomMetrics() {
        final int size = between(0, 10);
        final Map<Metric.Id, Measurement> result = new TreeMap<>();
        while (result.size() < size) {
            final Metric.Id id = new Metric.Id(
                randomFrom("ColumNAR", "Second"),
                randomAlphaOfLength(between(1, 6)).toLowerCase(Locale.ROOT),
                randomAlphaOfLength(between(1, 8)).toLowerCase(Locale.ROOT) + "_time_nanos"
            );
            final long total = randomNonNegativeLong();
            final long sampled = randomLongBetween(0, total);
            result.put(
                id,
                randomBoolean()
                    ? new CounterMeasurement(CounterMetric.of(id, Metric.Unit.COUNT, randomAlphaOfLength(5)), randomNonNegativeLong())
                    : new TimerMeasurement(
                        TimerMetric.of(id, randomFrom(TimingPolicy.values()), randomAlphaOfLength(5)),
                        randomNonNegativeLong(),
                        OptionalLong.empty(),
                        sampled,
                        total,
                        sampled < total || randomBoolean()
                    )
            );
        }
        return Snapshot.of(result.values());
    }

    private Map<String, Integer> randomReadersBuiltOrConvertersUsed() {
        int size = between(0, 10);
        Map<String, Integer> result = new TreeMap<>();
        while (result.size() < size) {
            result.put(randomAlphaOfLength(4), between(0, Integer.MAX_VALUE));
        }
        return result;
    }

    @Override
    protected ValuesSourceReaderOperatorStatus mutateInstance(ValuesSourceReaderOperatorStatus instance) throws IOException {
        Map<String, Integer> readersBuilt = instance.readersBuilt();
        Map<String, Integer> convertersUsed = instance.convertersUsed();
        long processNanos = instance.processNanos();
        int pagesReceived = instance.pagesReceived();
        int pagesEmitted = instance.pagesEmitted();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        long valuesLoaded = instance.valuesLoaded();
        long bytesRead = instance.bytesRead();
        long bytesDecoded = instance.bytesDecoded();
        long sourceDocsLoaded = instance.sourceDocsLoaded();
        long sourceFieldReads = instance.sourceFieldReads();
        long sourceBytesLoaded = instance.sourceBytesLoaded();
        CodecReads codecReads = instance.codecReads();
        switch (between(0, 13)) {
            case 0 -> readersBuilt = randomValueOtherThan(readersBuilt, this::randomReadersBuiltOrConvertersUsed);
            case 1 -> convertersUsed = randomValueOtherThan(convertersUsed, this::randomReadersBuiltOrConvertersUsed);
            case 2 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 4 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 5 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 6 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 7 -> valuesLoaded = randomValueOtherThan(valuesLoaded, ESTestCase::randomNonNegativeLong);
            case 8 -> bytesRead = randomValueOtherThan(bytesRead, ESTestCase::randomNonNegativeLong);
            case 9 -> bytesDecoded = randomValueOtherThan(bytesDecoded, ESTestCase::randomNonNegativeLong);
            case 10 -> sourceDocsLoaded = randomValueOtherThan(sourceDocsLoaded, ESTestCase::randomNonNegativeLong);
            case 11 -> sourceFieldReads = randomValueOtherThan(sourceFieldReads, ESTestCase::randomNonNegativeLong);
            case 12 -> sourceBytesLoaded = randomValueOtherThan(sourceBytesLoaded, ESTestCase::randomNonNegativeLong);
            case 13 -> codecReads = randomValueOtherThan(codecReads, this::randomCodecReads);
            default -> throw new UnsupportedOperationException();
        }
        return new ValuesSourceReaderOperatorStatus(
            readersBuilt,
            convertersUsed,
            processNanos,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            valuesLoaded,
            bytesRead,
            bytesDecoded,
            sourceDocsLoaded,
            sourceFieldReads,
            sourceBytesLoaded,
            codecReads
        );
    }
}
