/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Measurement;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TimerMeasurement;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CodecReadsTests extends AbstractWireSerializingTestCase<CodecReads> {

    private static final CounterMetric READ = CounterMetric.of("ColumNAR", "blocks", "read", Metric.Unit.COUNT, "blocks fetched");
    private static final TimerMetric READ_TIME = TimerMetric.of("ColumNAR", "blocks", "read_time_nanos", TimingPolicy.ADAPTIVE, "fetch")
        .withCpuTime("read_cpu_time_nanos", "fetch CPU");
    private static final TimerMetric OPEN_TIME = TimerMetric.of("ColumNAR", "readers", "open_time_nanos", TimingPolicy.EXACT, "open");

    public void testEveryDefinitionCrossesTheWireWholeAndIsValidatedOnArrival() throws IOException {
        for (int round = 0; round < 200; round++) {
            final CodecReads reads = createTestInstance();
            final CodecReads copy = copyInstance(reads);
            assertThat(copy, equalTo(reads));
            for (Map.Entry<String, Snapshot> codec : reads.totals().entrySet()) {
                final List<Measurement> sent = codec.getValue().measurements();
                final List<Measurement> received = copy.totals().get(codec.getKey()).measurements();
                for (int i = 0; i < sent.size(); i++) {
                    assertThat(
                        "the unit, policy, companion and description arrive",
                        received.get(i).definition(),
                        equalTo(sent.get(i).definition())
                    );
                }
            }
        }
    }

    public void testAWellFormedMetricTheReaderNeverHeardOfIsKeptAndRendered() throws IOException {
        final CounterMetric future = CounterMetric.of("Future", "new_component", "events", Metric.Unit.COUNT, "something new");
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", Snapshot.of(List.of(new CounterMeasurement(future, 7)))),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads copy = copyInstance(reads);
        assertThat(copy, equalTo(reads));
        assertThat(copy.totals().get("ColumNAR").get(future).definition().description(), equalTo("something new"));
        assertThat(copy.renderTotals(), equalTo(Map.of("ColumNAR", Map.of("Future.new_component", Map.of("events", 7L)))));
    }

    public void testAnInvalidDefinitionFailsTheRead() throws IOException {
        for (String key : List.of("a_feature_added_next_year_events", "ColumNAR.blocks.Upper")) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(1);
                out.writeString("ColumNAR");
                out.writeVInt(1);
                out.writeByte((byte) 0);
                out.writeString(key);
                out.writeEnum(Metric.Unit.COUNT);
                out.writeString("d");
                out.writeVLong(1);
                out.writeVInt(0);
                out.writeVInt(0);
                try (StreamInput in = out.bytes().streamInput()) {
                    final IllegalStateException e = expectThrows(IllegalStateException.class, () -> new CodecReads(in));
                    assertThat(e.getMessage(), containsString("[" + key + "]"));
                }
            }
        }
    }

    public void testAPairReadByTwoCodecsIsACodecBug() {
        final CodecReads left = new CodecReads(
            Map.of("A", Snapshot.EMPTY),
            List.of(detail("_1", "A", Snapshot.EMPTY)),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads right = new CodecReads(
            Map.of("B", Snapshot.EMPTY),
            List.of(detail("_1", "B", Snapshot.EMPTY)),
            Map.of(),
            false,
            true,
            true
        );
        expectThrows(IllegalStateException.class, () -> CodecReads.merge(List.of(left, right)));
    }

    public void testThePartsOfOnePairAreSummedIntoOneEntryAndTheTotalsByCodec() {
        final CodecReads left = new CodecReads(
            Map.of("ColumNAR", counted(5)),
            List.of(detail("_1", "ColumNAR", counted(2))),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads right = new CodecReads(
            Map.of("ColumNAR", counted(1), "Other", counted(4)),
            List.of(detail("_1", "ColumNAR", counted(1)), detail("_2", "ColumNAR", Snapshot.EMPTY)),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads merged = CodecReads.merge(List.of(left, right));
        assertThat(merged.totals(), equalTo(Map.of("ColumNAR", counted(6), "Other", counted(4))));
        assertThat(merged.details(), equalTo(List.of(detail("_1", "ColumNAR", counted(3)), detail("_2", "ColumNAR", Snapshot.EMPTY))));
    }

    public void testTheTotalsOfASegmentRenderByCodecWithNoFields() {
        final CodecReads.SegmentContext context = randomSegmentContext();
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", counted(7), "Other", counted(2)),
            List.of(
                new CodecReads.Detail("index:0/_1", null, "Other", null, Map.of(), counted(2)),
                new CodecReads.Detail("index:0/_1", null, "ColumNAR", null, Map.of(), counted(5))
            ),
            Map.of("index:0/_1", context),
            false,
            true,
            true
        );
        final Map<?, ?> segment = (Map<?, ?>) reads.renderSegments().get("index:0/_1");
        assertFalse("a segment's totals report no field", segment.containsKey("fields"));
        assertThat(segment.get("index"), equalTo(context.index()));
        assertThat(
            segment.get("metrics"),
            equalTo(Map.of("ColumNAR", Map.of("blocks", Map.of("read", 5L)), "Other", Map.of("ColumNAR.blocks", Map.of("read", 2L))))
        );
        assertThat(List.copyOf(((Map<?, ?>) segment.get("metrics")).keySet()), equalTo(List.of("ColumNAR", "Other")));
    }

    public void testThePartsOfTheTotalsOfASegmentAreSummedByCodec() {
        final CodecReads left = new CodecReads(
            Map.of("ColumNAR", counted(3), "Other", counted(1)),
            List.of(
                new CodecReads.Detail("_1", null, "ColumNAR", null, Map.of(), counted(3)),
                new CodecReads.Detail("_1", null, "Other", null, Map.of(), counted(1))
            ),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads right = new CodecReads(
            Map.of("ColumNAR", counted(4)),
            List.of(new CodecReads.Detail("_1", null, "ColumNAR", null, Map.of(), counted(4))),
            Map.of(),
            false,
            true,
            true
        );
        assertThat(
            "two codecs in one segment are two entries, not a codec bug",
            CodecReads.merge(List.of(left, right)).details(),
            equalTo(
                List.of(
                    new CodecReads.Detail("_1", null, "ColumNAR", null, Map.of(), counted(7)),
                    new CodecReads.Detail("_1", null, "Other", null, Map.of(), counted(1))
                )
            )
        );
    }

    public void testADescriptionKeepsWhatThePartsAgreeOnAndAConflictStaysAConflict() throws IOException {
        final CodecReads.SegmentContext segment = randomSegmentContext();
        final CodecReads.FieldContext field = randomFieldContext();
        final CodecReads withTrue = new CodecReads(
            Map.of("ColumNAR", counted(1)),
            List.of(new CodecReads.Detail("_1", "f", "ColumNAR", field.render(), Map.of("skip_index", CodecReads.Fact.TRUE), counted(1))),
            Map.of("_1", segment),
            false,
            true,
            true
        );
        final CodecReads agreeing = CodecReads.merge(List.of(withTrue, withTrue));
        assertThat(agreeing.segments(), equalTo(Map.of("_1", segment)));
        assertThat(agreeing.details().get(0).fieldInfo(), equalTo(field.render()));
        assertThat(agreeing.details().get(0).facts(), equalTo(Map.of("skip_index", CodecReads.Fact.TRUE)));

        final CodecReads silent = new CodecReads(
            Map.of("ColumNAR", counted(1)),
            List.of(detail("_1", "ColumNAR", counted(1))),
            Map.of(),
            false,
            true,
            true
        );
        final CodecReads partly = CodecReads.merge(List.of(withTrue, silent));
        assertThat("a part without a description says nothing either way", partly.segments(), equalTo(Map.of("_1", segment)));
        assertThat(partly.details().get(0).fieldInfo(), equalTo(field.render()));
        assertThat(partly.details().get(0).facts(), equalTo(Map.of("skip_index", CodecReads.Fact.TRUE)));
        assertThat("the measurements are merged all the same", partly.details().get(0).reads(), equalTo(counted(2)));

        final CodecReads.FieldContext other = randomValueOtherThan(field, CodecReadsTests::randomFieldContext);
        final Map<String, Object> shared = new java.util.LinkedHashMap<>();
        field.render().forEach((key, value) -> {
            if (java.util.Objects.equals(value, other.render().get(key))) {
                shared.put(key, value);
            }
        });
        final CodecReads withFalse = new CodecReads(
            Map.of("ColumNAR", counted(1)),
            List.of(new CodecReads.Detail("_1", "f", "ColumNAR", other.render(), Map.of("skip_index", CodecReads.Fact.FALSE), counted(1))),
            Map.of("_1", randomValueOtherThan(segment, CodecReadsTests::randomSegmentContext)),
            false,
            true,
            true
        );
        final CodecReads conflict = CodecReads.merge(List.of(withTrue, withFalse));
        final CodecReads staged = CodecReads.merge(List.of(conflict, withTrue));
        for (CodecReads merged : List.of(conflict, staged, CodecReads.merge(List.of(withTrue, withFalse, withTrue)))) {
            final CodecReads.Detail pair = merged.details().get(0);
            assertThat(
                "a conflict stays a conflict through later merges",
                pair.facts(),
                equalTo(Map.of("skip_index", CodecReads.Fact.CONFLICT))
            );
            assertThat("the field keeps the keys both describe alike, and only those", pair.fieldInfo(), equalTo(shared));
            assertTrue(merged.segments().containsKey("_1"));
            assertThat(merged.segments().get("_1"), nullValue());
            final Map<?, ?> rendered = (Map<?, ?>) merged.renderSegments().get("_1");
            assertThat("a segment in conflict is not described", List.copyOf(rendered.keySet()), equalTo(List.of("metrics", "fields")));
            final Map<?, ?> renderedField = (Map<?, ?>) ((Map<?, ?>) rendered.get("fields")).get("f");
            assertFalse("a fact in conflict is not reported", renderedField.containsKey("column"));
            assertThat(renderedField.get("field_info"), equalTo(shared.isEmpty() ? null : shared));
        }
        assertThat(copyInstance(staged), equalTo(staged));
    }

    public void testAFieldReadInSegmentsThatDescribeItDifferentlyKeepsWhatTheyShare() {
        final CodecReads.FieldContext first = randomFieldContext();
        final CodecReads.FieldContext second = new CodecReads.FieldContext(
            first.number() + 1,
            first.docValuesType(),
            first.docValuesGen(),
            first.docValuesSkipIndexType(),
            first.indexOptions(),
            first.postingsFormat(),
            first.hasNorms(),
            first.omitsNorms(),
            first.hasTermVectors(),
            first.hasPayloads(),
            first.pointDimensionCount(),
            first.pointIndexDimensionCount(),
            first.pointNumBytes(),
            first.vectorDimension(),
            first.vectorEncoding(),
            first.vectorSimilarity(),
            first.softDeletesField(),
            first.parentField(),
            first.sortPosition()
        );
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", counted(2)),
            List.of(
                new CodecReads.Detail("logs-a:0/_1", "f", "ColumNAR", first.render(), Map.of(), counted(1)),
                new CodecReads.Detail("logs-b:0/_1", "f", "ColumNAR", second.render(), Map.of(), counted(1))
            ),
            Map.of(),
            true,
            true,
            true
        );
        final Map<?, ?> fieldInfo = (Map<?, ?>) ((Map<?, ?>) reads.renderFields().get("f")).get("field_info");
        final Map<String, Object> expected = new java.util.LinkedHashMap<>(first.render());
        expected.remove("number");
        assertThat("two indices number the field differently and agree on the rest", fieldInfo, equalTo(expected));
        assertThat("the order of the keys is kept", List.copyOf(fieldInfo.keySet()), equalTo(List.copyOf(expected.keySet())));
    }

    public void testTheContextsRenderWhereTheyBelong() {
        final CodecReads.SegmentContext segment = new CodecReads.SegmentContext(
            "logs-a",
            3,
            100,
            98,
            90,
            "Elasticsearch",
            "10.5.2",
            true,
            List.of(
                new CodecReads.SortKey("host.name", "STRING", "asc", "min", "last"),
                new CodecReads.SortKey("@timestamp", "LONG", "desc", "max", "last")
            ),
            new CodecReads.FieldInfosSummary(3, 2, Map.of("ColumNAR", 1, "Lucene90", 1), false, true, false, null, "__root_doc_for_nested")
        );
        final CodecReads.FieldContext field = new CodecReads.FieldContext(
            4,
            "BINARY",
            -1,
            "NONE",
            "NONE",
            null,
            false,
            true,
            false,
            false,
            0,
            0,
            0,
            0,
            null,
            null,
            false,
            false,
            0
        );
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", counted(3)),
            List.of(
                new CodecReads.Detail("_1", "host.name", "ColumNAR", field.render(), Map.of("skip_index", CodecReads.Fact.TRUE), counted(3))
            ),
            Map.of("_1", segment),
            false,
            true,
            true
        );
        final Map<String, Object> fieldInfo = new LinkedHashMap<>();
        fieldInfo.put("number", 4);
        fieldInfo.put("doc_values_type", "BINARY");
        fieldInfo.put("doc_values_gen", -1L);
        fieldInfo.put("doc_values_skip_index_type", "NONE");
        fieldInfo.put("index_options", "NONE");
        fieldInfo.put("has_norms", false);
        fieldInfo.put("omits_norms", true);
        fieldInfo.put("has_term_vectors", false);
        fieldInfo.put("has_payloads", false);
        fieldInfo.put("point_dimension_count", 0);
        fieldInfo.put("point_index_dimension_count", 0);
        fieldInfo.put("point_num_bytes", 0);
        fieldInfo.put("vector_dimension", 0);
        fieldInfo.put("soft_deletes_field", false);
        fieldInfo.put("parent_field", false);
        fieldInfo.put("sort_field", true);
        fieldInfo.put("sort_position", 0);
        final Map<String, Object> expectedField = new LinkedHashMap<>();
        expectedField.put("field_info", fieldInfo);
        expectedField.put("column", Map.of("skip_index", true));
        expectedField.put("metrics", Map.of("ColumNAR", Map.of("blocks", Map.of("read", 3L))));
        final Map<String, Object> fieldInfos = new LinkedHashMap<>();
        fieldInfos.put("count", 3);
        fieldInfos.put("doc_values_count", 2);
        fieldInfos.put("doc_values_formats", Map.of("ColumNAR", 1, "Lucene90", 1));
        fieldInfos.put("has_norms", false);
        fieldInfos.put("has_points", true);
        fieldInfos.put("has_vectors", false);
        fieldInfos.put("parent_field", "__root_doc_for_nested");
        final Map<String, Object> expectedSegment = new LinkedHashMap<>();
        expectedSegment.put("index", "logs-a");
        expectedSegment.put("shard", 3);
        expectedSegment.put("max_doc", 100);
        expectedSegment.put("num_docs", 98);
        expectedSegment.put("root_docs", 90);
        expectedSegment.put("nested_docs", 10);
        expectedSegment.put("lucene_codec", "Elasticsearch");
        expectedSegment.put("lucene_version", "10.5.2");
        expectedSegment.put("compound_file", true);
        expectedSegment.put(
            "index_sort",
            List.of(
                Map.of("field", "host.name", "type", "STRING", "order", "asc", "mode", "min", "missing", "last"),
                Map.of("field", "@timestamp", "type", "LONG", "order", "desc", "mode", "max", "missing", "last")
            )
        );
        expectedSegment.put("field_infos", fieldInfos);
        expectedSegment.put("metrics", Map.of("ColumNAR", Map.of("blocks", Map.of("read", 3L))));
        expectedSegment.put("fields", Map.of("host.name", expectedField));
        final Map<String, Object> rendered = reads.renderSegments();
        assertThat(rendered, equalTo(Map.of("_1", expectedSegment)));
        final Map<?, ?> renderedSegment = (Map<?, ?>) rendered.get("_1");
        assertThat(List.copyOf(renderedSegment.keySet()), equalTo(List.copyOf(expectedSegment.keySet())));
        final Map<?, ?> renderedField = (Map<?, ?>) ((Map<?, ?>) renderedSegment.get("fields")).get("host.name");
        assertThat(List.copyOf(renderedField.keySet()), equalTo(List.of("field_info", "column", "metrics")));
        assertThat(List.copyOf(((Map<?, ?>) renderedField.get("field_info")).keySet()), equalTo(List.copyOf(fieldInfo.keySet())));
        assertThat(
            "the measurements are unchanged",
            reads.renderTotals(),
            equalTo(Map.of("ColumNAR", Map.of("blocks", Map.of("read", 3L))))
        );
    }

    public void testWhatIsUnknownOrAbsentIsLeftOut() {
        final CodecReads.SegmentContext segment = new CodecReads.SegmentContext(
            null,
            null,
            100,
            100,
            null,
            "Elasticsearch",
            "10.5.2",
            false,
            List.of(),
            new CodecReads.FieldInfosSummary(1, 1, Map.of(), false, false, false, null, null)
        );
        final Map<String, Object> rendered = new LinkedHashMap<>();
        segment.render(rendered);
        assertFalse("the reader names no shard", rendered.containsKey("index") || rendered.containsKey("shard"));
        assertFalse("blocks not counted", rendered.containsKey("root_docs") || rendered.containsKey("nested_docs"));
        assertFalse("not sorted", rendered.containsKey("index_sort"));
        assertThat(
            ((Map<?, ?>) rendered.get("field_infos")).keySet(),
            equalTo(java.util.Set.of("count", "doc_values_count", "doc_values_formats", "has_norms", "has_points", "has_vectors"))
        );
        final CodecReads.FieldContext field = new CodecReads.FieldContext(
            1,
            "BINARY",
            -1,
            "NONE",
            "NONE",
            null,
            false,
            false,
            false,
            false,
            0,
            0,
            0,
            0,
            null,
            null,
            false,
            false,
            null
        );
        final Map<String, Object> info = field.render();
        assertThat(info.get("sort_field"), equalTo(false));
        for (String absent : List.of("sort_position", "postings_format", "vector_encoding", "vector_similarity")) {
            assertFalse(absent, info.containsKey(absent));
        }
    }

    public void testATimerIsEstimatedOnlyWhenACallWentUnmeasured() {
        final long calls = randomLongBetween(1, 1_000_000);
        final long sampled = randomLongBetween(0, calls - 1);
        final TimerMetric exact = TimerMetric.of("ColumNAR", "readers", "open_time_nanos", TimingPolicy.EXACT, "open");
        assertThat(
            timerOf(new TimerMeasurement(exact, 0, OptionalLong.empty(), 0, 0, false)),
            equalTo(Map.of("value", 0L, "unit", "ns", "estimated", false, "total_calls", 0L))
        );
        assertThat(
            "a sampling timer that measured every call is exact",
            timerOf(new TimerMeasurement(OPEN_TIME, 1039, OptionalLong.empty(), calls, calls, false)),
            equalTo(Map.of("value", 1039L, "unit", "ns", "estimated", false, "total_calls", calls))
        );
        assertThat(
            timerOf(new TimerMeasurement(OPEN_TIME, 1039, OptionalLong.empty(), sampled, calls, true)),
            equalTo(Map.of("value", 1039L, "unit", "ns", "estimated", true, "total_calls", calls, "sampled_calls", sampled))
        );
        assertThat(
            "a value merged from a sampled part stays an estimate when saturated counts cannot show it",
            timerOf(new TimerMeasurement(OPEN_TIME, 1, OptionalLong.empty(), Long.MAX_VALUE, Long.MAX_VALUE, true)),
            equalTo(Map.of("value", 1L, "unit", "ns", "estimated", true, "total_calls", Long.MAX_VALUE, "sampled_calls", Long.MAX_VALUE))
        );
    }

    public void testACpuTimeRendersUnderItsOwnNameWithTheCallsOfItsTimer() {
        final long calls = randomLongBetween(2, 10_000);
        final long sampled = randomLongBetween(1, calls - 1);
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", Snapshot.of(List.of(new TimerMeasurement(READ_TIME, 900, OptionalLong.of(600), sampled, calls, true)))),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final Map<?, ?> blocks = (Map<?, ?>) ((Map<?, ?>) reads.renderTotals().get("ColumNAR")).get("blocks");
        assertThat(
            blocks.get("read_time_nanos"),
            equalTo(Map.of("value", 900L, "unit", "ns", "estimated", true, "total_calls", calls, "sampled_calls", sampled))
        );
        assertThat(
            blocks.get("read_cpu_time_nanos"),
            equalTo(Map.of("value", 600L, "unit", "ns", "estimated", true, "total_calls", calls, "sampled_calls", sampled))
        );
        final CodecReads withoutCpu = new CodecReads(
            Map.of("ColumNAR", Snapshot.of(List.of(new TimerMeasurement(READ_TIME, 900, OptionalLong.empty(), calls, calls, false)))),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final Map<?, ?> elapsedOnly = (Map<?, ?>) ((Map<?, ?>) withoutCpu.renderTotals().get("ColumNAR")).get("blocks");
        assertThat("no CPU time was measured, so none is shown", List.copyOf(elapsedOnly.keySet()), equalTo(List.of("read_time_nanos")));
    }

    public void testTimersRenderAsObjectsAndCountsAsNumbers() {
        final CounterMetric other = CounterMetric.of("Other", "pages", "read", Metric.Unit.COUNT, "pages");
        final CodecReads reads = new CodecReads(
            Map.of(
                "ColumNAR",
                Snapshot.of(
                    List.of(
                        new CounterMeasurement(READ, 3),
                        new TimerMeasurement(OPEN_TIME, 740, OptionalLong.empty(), 2, 2, false),
                        new CounterMeasurement(other, 1)
                    )
                )
            ),
            List.of(detail("_1", "ColumNAR", counted(2))),
            Map.of(),
            false,
            true,
            true
        );
        assertThat(
            reads.renderTotals(),
            equalTo(
                Map.of(
                    "ColumNAR",
                    Map.of(
                        "blocks",
                        Map.of("read", 3L),
                        "readers",
                        Map.of("open_time_nanos", Map.of("value", 740L, "unit", "ns", "estimated", false, "total_calls", 2L)),
                        "Other.pages",
                        Map.of("read", 1L)
                    )
                )
            )
        );
        assertThat(
            reads.renderSegments(),
            equalTo(Map.of("_1", Map.of("metrics", blocksRead(2), "fields", Map.of("f", Map.of("metrics", blocksRead(2))))))
        );
        assertThat(CodecReads.EMPTY.renderTotals(), equalTo(Map.of()));
        assertThat(CodecReads.EMPTY.renderSegments(), equalTo(Map.of()));
    }

    public void testTheTotalsOfEachCodecAreKeptApartAndOrderedByCodec() {
        final CodecReads reads = new CodecReads(
            Map.of("Zeta", counted(2), "ColumNAR", counted(3), "Alpha_2", counted(5)),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final Map<String, Object> rendered = reads.renderTotals();
        assertThat(List.copyOf(rendered.keySet()), equalTo(List.of("Alpha_2", "ColumNAR", "Zeta")));
        assertThat(
            "a codec's totals are its own, never added to another's",
            rendered.get("Alpha_2"),
            equalTo(Map.of("ColumNAR.blocks", Map.of("read", 5L)))
        );
        assertThat(rendered.get("Zeta"), equalTo(Map.of("ColumNAR.blocks", Map.of("read", 2L))));
        assertThat(rendered.get("ColumNAR"), equalTo(Map.of("blocks", Map.of("read", 3L))));
    }

    public void testComponentsAndNamesAreRenderedInOrder() {
        final List<Measurement> measurements = new ArrayList<>();
        for (String component : List.of("z", "blocks", "a", "blocks_x")) {
            for (String name : List.of("zz", "read", "a")) {
                measurements.add(new CounterMeasurement(CounterMetric.of("ColumNAR", component, name, Metric.Unit.COUNT, "d"), 1));
            }
        }
        final Map<?, ?> total = (Map<?, ?>) new CodecReads(
            Map.of("ColumNAR", Snapshot.of(measurements)),
            List.of(),
            Map.of(),
            false,
            true,
            true
        ).renderTotals().get("ColumNAR");
        assertThat(List.copyOf(total.keySet()), equalTo(List.of("a", "blocks", "blocks_x", "z")));
        assertThat(List.copyOf(((Map<?, ?>) total.get("blocks")).keySet()), equalTo(List.of("a", "read", "zz")));
    }

    private static Object timerOf(TimerMeasurement measurement) {
        final CodecReads reads = new CodecReads(
            Map.of("ColumNAR", Snapshot.of(List.of(measurement))),
            List.of(),
            Map.of(),
            false,
            true,
            true
        );
        final Map<?, ?> metrics = (Map<?, ?>) reads.renderTotals().get("ColumNAR");
        return ((Map<?, ?>) metrics.get(measurement.id().component())).get(measurement.id().name());
    }

    private static CodecReads.Detail detail(String segment, String codec, Snapshot reads) {
        return new CodecReads.Detail(segment, "f", codec, null, Map.of(), reads);
    }

    private static Map<String, Object> blocksRead(long blocks) {
        return Map.of("ColumNAR", Map.of("blocks", Map.of("read", blocks)));
    }

    private static Snapshot counted(long blocks) {
        return Snapshot.of(List.of(new CounterMeasurement(READ, blocks)));
    }

    @Override
    protected Writeable.Reader<CodecReads> instanceReader() {
        return CodecReads::new;
    }

    @Override
    protected CodecReads createTestInstance() {
        final List<String> codecs = randomSubsetOf(between(0, 3), List.of("ColumNAR", "Numeric", "Other_2"));
        final Map<String, Snapshot> totals = new TreeMap<>();
        for (String codec : codecs) {
            totals.put(codec, randomSnapshot(codec));
        }
        final List<CodecReads.Detail> details = new ArrayList<>();
        final Map<String, CodecReads.SegmentContext> segments = new TreeMap<>();
        if (codecs.isEmpty() == false) {
            for (int i = 0; i < between(0, 5); i++) {
                final String codec = randomFrom(codecs);
                final String segment = randomFrom("", "index:0/") + "_" + i;
                if (randomBoolean()) {
                    segments.put(segment, randomBoolean() ? null : randomSegmentContext());
                }
                switch (between(0, 2)) {
                    case 0 -> {
                        details.add(new CodecReads.Detail(segment, null, codec, null, Map.of(), randomSnapshot(codec)));
                        continue;
                    }
                    case 1 -> {
                        // NOTE: A field reported over the segments that are not selected has no segment.
                        details.add(
                            new CodecReads.Detail(
                                null,
                                "field_only_" + i,
                                codec,
                                randomBoolean() ? null : randomFieldContext().render(),
                                Map.of(),
                                randomSnapshot(codec)
                            )
                        );
                        continue;
                    }
                    default -> {
                    }
                }
                final Map<String, CodecReads.Fact> facts = new TreeMap<>();
                if (randomBoolean()) {
                    facts.put("skip_index", randomFrom(CodecReads.Fact.values()));
                }
                details.add(
                    new CodecReads.Detail(
                        segment,
                        "field_" + between(0, 3),
                        codec,
                        randomBoolean() ? null : randomFieldContext().render(),
                        facts,
                        randomSnapshot(codec)
                    )
                );
            }
        }
        return new CodecReads(totals, details, segments, randomBoolean(), randomBoolean(), randomBoolean());
    }

    @Override
    protected CodecReads mutateInstance(CodecReads instance) {
        final Map<String, Snapshot> totals = new TreeMap<>(instance.totals());
        totals.put("Mutated", Snapshot.of(List.of(new CounterMeasurement(READ, randomNonNegativeLong()))));
        return new CodecReads(totals, instance.details(), Map.of(), false, true, true);
    }

    private static CodecReads.SegmentContext randomSegmentContext() {
        final int maxDoc = between(0, 1_000_000);
        final Map<String, Integer> formats = new TreeMap<>();
        for (String format : randomSubsetOf(List.of("ColumNAR", "Lucene90", "ES819TSDB"))) {
            formats.put(format, between(1, 30));
        }
        final List<CodecReads.SortKey> sort = new ArrayList<>();
        for (int i = 0; i < between(0, 3); i++) {
            sort.add(
                new CodecReads.SortKey(
                    randomAlphaOfLength(5),
                    randomFrom("STRING", "LONG"),
                    randomFrom("asc", "desc"),
                    randomFrom("min", "max", null),
                    randomFrom("first", "last", "42", null)
                )
            );
        }
        return new CodecReads.SegmentContext(
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomBoolean() ? null : between(0, 20),
            maxDoc,
            between(0, maxDoc),
            randomBoolean() ? null : between(0, maxDoc),
            randomAlphaOfLength(8),
            "10.5.2",
            randomBoolean(),
            sort,
            new CodecReads.FieldInfosSummary(
                between(0, 100),
                between(0, 100),
                formats,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomFrom("__soft_deletes", null),
                randomFrom("__root_doc_for_nested", null)
            )
        );
    }

    private static CodecReads.FieldContext randomFieldContext() {
        final int vectorDimension = randomFrom(0, between(1, 1024));
        return new CodecReads.FieldContext(
            between(0, 200),
            randomFrom("NONE", "BINARY", "SORTED_SET"),
            randomLongBetween(-1, 10),
            randomFrom("NONE", "RANGE"),
            randomFrom("NONE", "DOCS"),
            randomFrom("ES812Postings", null),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            between(0, 4),
            between(0, 4),
            between(0, 16),
            vectorDimension,
            vectorDimension == 0 ? null : "FLOAT32",
            vectorDimension == 0 ? null : "COSINE",
            randomBoolean(),
            randomBoolean(),
            randomFrom(0, 1, null)
        );
    }

    private static Snapshot randomSnapshot(String codec) {
        final Map<Metric.Id, Measurement> measurements = new TreeMap<>();
        for (int i = 0; i < between(0, 8); i++) {
            final String group = randomBoolean() ? codec : randomFrom("Second", "ColumNAR", "Z9");
            final String component = randomFrom("a", "blocks", "blocks_x", "pages", "z");
            final String name = randomFrom("read", "read_x", "events", "a", "zz");
            if (randomBoolean()) {
                final Metric.Unit unit = randomFrom(Metric.Unit.COUNT, Metric.Unit.BYTES);
                final CounterMetric counter = CounterMetric.of(
                    group,
                    component,
                    name + (unit == Metric.Unit.BYTES ? "_bytes" : ""),
                    unit,
                    randomAlphaOfLength(8)
                );
                measurements.put(counter.id(), new CounterMeasurement(counter, randomNonNegativeLong()));
            } else {
                TimerMetric timer = TimerMetric.of(
                    group,
                    component,
                    name + "_time_nanos",
                    randomFrom(TimingPolicy.values()),
                    randomAlphaOfLength(8)
                );
                if (randomBoolean()) {
                    timer = timer.withCpuTime(name + "_cpu_time_nanos", randomAlphaOfLength(8));
                }
                final long total = randomNonNegativeLong();
                final long sampled = randomLongBetween(0, total);
                final OptionalLong cpu = timer.cpuTime() != null && randomBoolean()
                    ? OptionalLong.of(randomNonNegativeLong())
                    : OptionalLong.empty();
                measurements.put(
                    timer.id(),
                    new TimerMeasurement(timer, randomNonNegativeLong(), cpu, sampled, total, sampled < total || randomBoolean())
                );
            }
        }
        final Map<Metric.Id, Measurement> distinct = new LinkedHashMap<>();
        final java.util.Set<Metric.Id> used = new java.util.HashSet<>();
        for (Measurement measurement : measurements.values()) {
            final Metric.Id cpu = measurement instanceof TimerMeasurement timer && timer.definition().cpuTime() != null
                ? timer.definition().cpuTime().id()
                : null;
            if (used.contains(measurement.id()) || (cpu != null && used.contains(cpu))) {
                continue;
            }
            used.add(measurement.id());
            if (cpu != null) {
                used.add(cpu);
            }
            distinct.put(measurement.id(), measurement);
        }
        return Snapshot.of(distinct.values());
    }
}
