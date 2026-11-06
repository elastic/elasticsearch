/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CodecReadAttributionTests extends ESTestCase {

    private static final CounterMetric READ = CounterMetric.of("ColumNAR", "blocks", "read", Metric.Unit.COUNT, "blocks read");
    private static final CounterMetric OTHER_READ = CounterMetric.of("Other", "blocks", "read", Metric.Unit.COUNT, "blocks read");
    private static final TimerMetric READ_TIME = TimerMetric.of("ColumNAR", "blocks", "read_time_nanos", TimingPolicy.ADAPTIVE, "t");

    private static final List<CounterMetric> COUNTERS = IntStream.range(0, 30)
        .mapToObj(i -> CounterMetric.of("ColumNAR", "component" + (i % 4), "count_" + i, Metric.Unit.COUNT, "c"))
        .toList();
    private static final List<TimerMetric> TIMERS = IntStream.range(0, 10)
        .mapToObj(i -> TimerMetric.of("ColumNAR", "component" + (i % 4), "t" + i + "_time_nanos", TimingPolicy.ADAPTIVE, "t"))
        .toList();

    public void testAnUnselectedReaderRecordsIntoItsCodecsAggregate() {
        final CodecReadAttribution attribution = new CodecReadAttribution(fields("a"), CodecReadAttribution.NO_CHARGE);
        final MetricRecorder b = attribution.metrics("ColumNAR", "_1", "b", Map.of());
        assertThat(attribution.metrics("ColumNAR", "_2", "c", Map.of()), sameInstance(b));
        b.counter(READ).add(4);
        attribution.metrics("ColumNAR", "_1", "a", Map.of()).counter(READ).add(1);
        final CodecReads reads = attribution.snapshot();
        assertThat(reads.totals().get("ColumNAR").get(READ), equalTo(new CounterMeasurement(READ, 5)));
        assertThat(reads.details(), equalTo(List.of(new CodecReads.Detail(null, "a", "ColumNAR", null, Map.of(), counted(1)))));
        assertThat(attribution.detailCount(), equalTo(1));
    }

    public void testASelectedPairWhoseReaderOpenedIsPresentWhenNothingWasCounted() {
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
        attribution.metrics("ColumNAR", "_1", "a", Map.of());
        assertThat(
            attribution.snapshot().details(),
            equalTo(List.of(new CodecReads.Detail("_1", "a", "ColumNAR", null, Map.of(), Snapshot.EMPTY)))
        );
    }

    public void testTheSameSegmentNameInTwoShardsGivesTwoPairs() {
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
        attribution.partition("index-a:0");
        final MetricRecorder first = attribution.metrics("ColumNAR", "_1", "f", Map.of());
        first.counter(READ).add(2);
        attribution.partition("index-b:0");
        final MetricRecorder second = attribution.metrics("ColumNAR", "_1", "f", Map.of());
        second.counter(READ).add(3);
        assertNotSame("a shard switch does not reuse the other shard's detail", first, second);
        assertThat(
            attribution.snapshot().details(),
            equalTo(
                List.of(
                    new CodecReads.Detail("index-a:0/_1", "f", "ColumNAR", null, Map.of(), counted(2)),
                    new CodecReads.Detail("index-b:0/_1", "f", "ColumNAR", null, Map.of(), counted(3))
                )
            )
        );
        assertThat(attribution.snapshot().totals().get("ColumNAR"), equalTo(counted(5)));
    }

    public void testAPairReadByTwoCodecsFailsWhenTheSecondReaderOpens() {
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
        attribution.metrics("ColumNAR", "_1", "f", Map.of());
        expectThrows(IllegalStateException.class, () -> attribution.metrics("Other", "_1", "f", Map.of()));
        assertThat(
            "an unselected pair has no identity to check",
            attribution.snapshot().totals().keySet(),
            equalTo(java.util.Set.of("ColumNAR", "Other"))
        );
    }

    public void testEveryViewIsTheSumOfTheReadsItCovers() {
        final List<String> segmentNames = List.of("_0", "_1", "_2", "_3");
        final List<String> fieldNames = List.of("f0", "f1", "f2", "f3", "f4");
        for (int round = 0; round < 20; round++) {
            final List<Object[]> reads = new ArrayList<>();
            for (int i = 0; i < between(1, 300); i++) {
                reads.add(new Object[] { randomFrom(segmentNames), randomFrom(fieldNames), (long) between(0, 10) });
            }
            final Chosen segmentSelection = Chosen.randomFrom(segmentNames);
            final Chosen fieldSelection = Chosen.randomFrom(fieldNames);
            final ProfileSelection selection = ProfileSelection.of(segmentSelection.names(), fieldSelection.names());
            final CodecReadAttribution attribution = new CodecReadAttribution(selection, CodecReadAttribution.NO_CHARGE);
            for (Object[] read : reads) {
                final String field = (String) read[1];
                attribution.metrics(codecOf(field), (String) read[0], field, Map.of()).counter(readOf(field)).add((Long) read[2]);
            }

            // NOTE: The oracle follows the selection contract, not the attribution: each dimension expands only itself.
            final Map<String, Map<String, Long>> totals = new java.util.TreeMap<>();
            final Map<String, Map<String, Long>> bySegment = new java.util.TreeMap<>();
            final Map<String, Map<String, Long>> byField = new java.util.TreeMap<>();
            final Map<String, Map<String, Long>> pairs = new java.util.TreeMap<>();
            for (Object[] read : reads) {
                final String segment = (String) read[0];
                final String field = (String) read[1];
                final long count = (Long) read[2];
                totals.computeIfAbsent(codecOf(field), c -> new java.util.TreeMap<>()).merge("blocks.read", count, Long::sum);
                final boolean segmentSelected = segmentSelection.includes(segment);
                final boolean fieldSelected = fieldSelection.includes(field);
                if (segmentSelected) {
                    bySegment.computeIfAbsent(segment, s -> new java.util.TreeMap<>()).merge(codecOf(field), count, Long::sum);
                }
                if (fieldSelected) {
                    byField.computeIfAbsent(field, f -> new java.util.TreeMap<>()).merge(codecOf(field), count, Long::sum);
                }
                if (segmentSelected && fieldSelected) {
                    pairs.computeIfAbsent(segment, s -> new java.util.TreeMap<>()).merge(field, count, Long::sum);
                }
            }

            final CodecReads snapshot = attribution.snapshot();
            final String where = selection + " over " + reads.size() + " reads";
            final Map<String, Map<String, Long>> renderedTotals = new java.util.TreeMap<>();
            snapshot.renderTotals().forEach((codec, metrics) -> renderedTotals.put(codec, Map.of("blocks.read", readCount(metrics))));
            assertThat("the totals count every read, whatever is selected: " + where, renderedTotals, equalTo(totals));

            final Map<String, Object> renderedSegments = snapshot.renderSegments();
            assertThat(where, renderedSegments.keySet(), equalTo(bySegment.keySet()));
            for (Map.Entry<String, Object> segment : renderedSegments.entrySet()) {
                final Map<?, ?> entry = (Map<?, ?>) segment.getValue();
                assertThat(where, byCodec(entry.get("metrics")), equalTo(bySegment.get(segment.getKey())));
                final Map<String, Long> expectedPairs = pairs.getOrDefault(segment.getKey(), Map.of());
                assertThat(
                    "a segment lists fields only when the field dimension selects them: " + where,
                    entry.containsKey("fields"),
                    equalTo(expectedPairs.isEmpty() == false)
                );
                final Map<String, Long> renderedPairs = new java.util.TreeMap<>();
                ((Map<?, ?>) (entry.containsKey("fields") ? entry.get("fields") : Map.of())).forEach(
                    (field, pair) -> renderedPairs.put(
                        (String) field,
                        byCodec(((Map<?, ?>) pair).get("metrics")).values().stream().mapToLong(Long::longValue).sum()
                    )
                );
                assertThat(where, renderedPairs, equalTo(expectedPairs));
            }

            final Map<String, Object> renderedFields = snapshot.renderFields();
            assertThat(where, renderedFields.keySet(), equalTo(byField.keySet()));
            for (Map.Entry<String, Object> field : renderedFields.entrySet()) {
                assertThat(where, byCodec(((Map<?, ?>) field.getValue()).get("metrics")), equalTo(byField.get(field.getKey())));
            }
        }
    }

    public void testWithEveryPairSelectedTheTotalsAreTheSumOfThePairsWhereverTheyAreSums() {
        final TimerMetric exactTime = TimerMetric.of("ColumNAR", "readers", "open_time_nanos", TimingPolicy.EXACT, "t");
        final TimerMetric adaptiveTime = TimerMetric.of("ColumNAR", "blocks", "read_time_nanos", TimingPolicy.ADAPTIVE, "t");
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
        final int calls = between(1, 5_000);
        for (int call = 0; call < calls; call++) {
            final MetricRecorder stats = attribution.metrics("ColumNAR", "_" + between(0, 3), "f" + between(0, 5), Map.of());
            stats.counter(READ).add(between(0, 10));
            for (Timer timer : List.of(stats.timer(exactTime), stats.timer(adaptiveTime))) {
                timer.start();
                timer.stop();
            }
        }
        final CodecReads reads = attribution.snapshot();
        final Snapshot totals = reads.totals().get("ColumNAR");
        long counted = 0;
        long exactValue = 0;
        long exactCalls = 0;
        long exactSampled = 0;
        long adaptiveCalls = 0;
        for (CodecReads.Detail detail : reads.details()) {
            counted += detail.reads().get(READ).value();
            exactValue += detail.reads().get(exactTime).elapsedNanos();
            exactCalls += detail.reads().get(exactTime).totalCalls();
            exactSampled += detail.reads().get(exactTime).sampledCalls();
            adaptiveCalls += detail.reads().get(adaptiveTime).totalCalls();
        }
        assertThat("a counter is a sum", totals.get(READ).value(), equalTo(counted));
        assertThat("an exact timer measures every call with the same reading", totals.get(exactTime).elapsedNanos(), equalTo(exactValue));
        assertThat(totals.get(exactTime).totalCalls(), equalTo(exactCalls));
        assertThat(totals.get(exactTime).sampledCalls(), equalTo(exactSampled));
        assertThat("every timer counts every call", totals.get(adaptiveTime).totalCalls(), equalTo(adaptiveCalls));
        assertThat(adaptiveCalls, equalTo((long) calls));
    }

    public void testTheTotalsOfASegmentAreKeptByCodecAcrossItsFields() {
        final AtomicLong charged = new AtomicLong();
        final CodecReadAttribution attribution = new CodecReadAttribution(segments("_1"), charged::addAndGet);
        final MetricRecorder columnar = attribution.metrics("ColumNAR", "_1", "f", Map.of("skip_index", true));
        assertThat(
            "every field of the segment shares its codec's detail",
            attribution.metrics("ColumNAR", "_1", "g", Map.of()),
            sameInstance(columnar)
        );
        assertThat(charged.get(), equalTo(CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES));
        columnar.counter(READ).add(2);
        attribution.metrics("ColumNAR", "_1", "g", Map.of("skip_index", false)).counter(READ).add(3);
        attribution.metrics("Other", "_1", "f", Map.of()).counter(READ).add(4);
        attribution.metrics("ColumNAR", "_2", "f", Map.of()).counter(READ).add(10);
        final CodecReads reads = attribution.snapshot();
        assertThat(
            "a codec that reads a field another codec read in the segment is no codec bug here",
            reads.details(),
            equalTo(
                List.of(
                    new CodecReads.Detail("_1", null, "ColumNAR", null, Map.of(), counted(5)),
                    new CodecReads.Detail("_1", null, "Other", null, Map.of(), counted(4))
                )
            )
        );
        assertThat(reads.totals().get("ColumNAR"), equalTo(counted(15)));
        assertThat(attribution.detailCount(), equalTo(2));
    }

    public void testANamedFieldIsTotalledOverEverySegmentWithoutExpandingThem() {
        final CodecReadAttribution attribution = new CodecReadAttribution(fields("f"), CodecReadAttribution.NO_CHARGE);
        attribution.partition("index:0");
        final MetricRecorder first = attribution.metrics("ColumNAR", "_1", "f", Map.of("skip_index", true));
        assertThat(
            "one detail serves the field in every segment",
            attribution.metrics("ColumNAR", "_2", "f", Map.of()),
            sameInstance(first)
        );
        first.counter(READ).add(3);
        attribution.metrics("Other", "_3", "f", Map.of()).counter(READ).add(4);
        attribution.metrics("ColumNAR", "_1", "g", Map.of()).counter(READ).add(5);
        final CodecReads reads = attribution.snapshot();
        assertThat(
            "codecs are kept apart, and with no descriptions asked for no fact is kept",
            reads.details(),
            equalTo(
                List.of(
                    new CodecReads.Detail(null, "f", "ColumNAR", null, Map.of(), counted(3)),
                    new CodecReads.Detail(null, "f", "Other", null, Map.of(), counted(4))
                )
            )
        );
        assertThat("no segment is described", reads.segments(), equalTo(Map.of()));
        assertThat(reads.renderSegments(), equalTo(Map.of()));
        assertThat(
            reads.renderFields(),
            equalTo(
                Map.of(
                    "f",
                    Map.of(
                        "metrics",
                        Map.of("ColumNAR", Map.of("blocks", Map.of("read", 3L)), "Other", Map.of("ColumNAR.blocks", Map.of("read", 4L)))
                    )
                )
            )
        );
        assertThat(reads.totals().get("ColumNAR"), equalTo(counted(8)));
    }

    public void testANamedSegmentAndFieldKeepTheirOwnTotalsAndTheirPair() {
        final CodecReadAttribution attribution = new CodecReadAttribution(
            ProfileSelection.of(Names.of(List.of("_1")), Names.of(List.of("f"))),
            CodecReadAttribution.NO_CHARGE
        );
        attribution.metrics("ColumNAR", "_1", "f", Map.of()).counter(READ).add(1);
        attribution.metrics("ColumNAR", "_1", "g", Map.of()).counter(READ).add(2);
        attribution.metrics("ColumNAR", "_2", "f", Map.of()).counter(READ).add(4);
        attribution.metrics("ColumNAR", "_2", "g", Map.of()).counter(READ).add(8);
        final CodecReads reads = attribution.snapshot();
        assertThat(
            "each reader is in one detail at most",
            reads.details(),
            equalTo(
                List.of(
                    new CodecReads.Detail(null, "f", "ColumNAR", null, Map.of(), counted(4)),
                    new CodecReads.Detail("_1", null, "ColumNAR", null, Map.of(), counted(2)),
                    new CodecReads.Detail("_1", "f", "ColumNAR", null, Map.of(), counted(1))
                )
            )
        );
        final Map<?, ?> segment = (Map<?, ?>) reads.renderSegments().get("_1");
        assertThat("the segment counts every field it read", segment.get("metrics"), equalTo(blocksRead(3)));
        assertThat(
            "the pair counts the field in the segment",
            segment.get("fields"),
            equalTo(Map.of("f", Map.of("metrics", blocksRead(1))))
        );
        assertThat(
            "the field counts every segment it was read in",
            reads.renderFields(),
            equalTo(Map.of("f", Map.of("metrics", blocksRead(5))))
        );
        assertThat(reads.totals().get("ColumNAR"), equalTo(counted(15)));
    }

    public void testTheFactsOfAPairAreKeptAndAConflictBetweenItsReadersIsKept() {
        final AtomicLong charged = new AtomicLong();
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, charged::addAndGet);
        attribution.metrics("ColumNAR", "_1", "agreed", Map.of("skip_index", true));
        attribution.metrics("ColumNAR", "_1", "agreed", Map.of("skip_index", true));
        attribution.metrics("ColumNAR", "_1", "torn", Map.of("skip_index", true));
        attribution.metrics("ColumNAR", "_1", "torn", Map.of("skip_index", false));
        attribution.metrics("ColumNAR", "_1", "torn", Map.of("skip_index", true));
        attribution.metrics("ColumNAR", "_1", "silent", Map.of());
        final Map<String, Map<String, CodecReads.Fact>> facts = new java.util.TreeMap<>();
        attribution.snapshot().details().forEach(detail -> facts.put(detail.field(), detail.facts()));
        assertThat(
            facts,
            equalTo(
                Map.of(
                    "agreed",
                    Map.of("skip_index", CodecReads.Fact.TRUE),
                    "torn",
                    Map.of("skip_index", CodecReads.Fact.CONFLICT),
                    "silent",
                    Map.of()
                )
            )
        );
        final Map<?, ?> fields = (Map<?, ?>) ((Map<?, ?>) attribution.snapshot().renderSegments().get("_1")).get("fields");
        assertThat(((Map<?, ?>) fields.get("agreed")).get("column"), equalTo(Map.of("skip_index", true)));
        assertFalse("a conflict is not reported", ((Map<?, ?>) fields.get("torn")).containsKey("column"));
        assertFalse(((Map<?, ?>) fields.get("silent")).containsKey("column"));
    }

    public void testEveryDetailAllocationIsChargedFirstAndARefusalLeavesNothing() {
        final AtomicLong charged = new AtomicLong();
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, charged::addAndGet);
        final MetricRecorder detail = attribution.metrics("ColumNAR", "_1", "f", Map.of());
        assertThat(charged.get(), equalTo((CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES)));
        detail.counter(READ);
        detail.timer(READ_TIME);
        assertThat(
            charged.get(),
            equalTo(
                (CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES) + TestCollectors.COUNTER_BYTES + TestCollectors.TIMER_BYTES
            )
        );
        attribution.metrics("ColumNAR", "_1", "f", Map.of());
        assertThat(
            "an existing pair is not charged again",
            charged.get(),
            equalTo(
                (CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES) + TestCollectors.COUNTER_BYTES + TestCollectors.TIMER_BYTES
            )
        );

        final CodecReadAttribution refusing = new CodecReadAttribution(ProfileSelection.EVERYTHING, bytes -> {
            throw new CircuitBreakingException("no memory", CircuitBreaker.Durability.TRANSIENT);
        });
        expectThrows(CircuitBreakingException.class, () -> refusing.metrics("ColumNAR", "_1", "f", Map.of()));
        assertThat(refusing.detailCount(), equalTo(0));
        assertThat(refusing.snapshot().details(), equalTo(List.of()));
    }

    public void testTheChargeEstimatesTheMemoryOfTheDetails() {
        for (int layout = 0; layout < 3; layout++) {
            final int counters = new int[] { 13, 20, 30 }[layout];
            final int timers = new int[] { 2, 10, 0 }[layout];
            final int pairs = between(5, 20);
            final AtomicLong charged = new AtomicLong();
            final CodecReadAttribution aggregateOnly = new CodecReadAttribution(ProfileSelection.NONE, CodecReadAttribution.NO_CHARGE);
            final CodecReadAttribution detailed = new CodecReadAttribution(ProfileSelection.EVERYTHING, charged::addAndGet);
            for (CodecReadAttribution attribution : List.of(aggregateOnly, detailed)) {
                for (int pair = 0; pair < pairs; pair++) {
                    register(attribution.metrics("ColumNAR", "_" + pair, "field_" + pair, Map.of()), counters, timers);
                }
            }
            final long measured = RamUsageTester.ramUsed(detailed) - RamUsageTester.ramUsed(aggregateOnly);
            final String message = "layout " + layout + ": charged " + charged.get() + " for " + measured + " measured";
            assertThat(message, charged.get(), greaterThanOrEqualTo(measured / 2));
            assertThat(message, charged.get(), lessThanOrEqualTo(measured * 2));
        }
    }

    public void testAFieldDescribedDifferentlyBySegmentsKeepsWhatTheyAgreeOnAndChargesItOnce() throws IOException {
        try (Directory first = newDirectory(); Directory second = newDirectory()) {
            // NOTE: The second index sees another field first, so it numbers "tag" differently.
            for (Directory directory : List.of(first, second)) {
                try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                    final Document document = new Document();
                    if (directory == second) {
                        document.add(new StringField("id", "1", Field.Store.NO));
                    }
                    document.add(new SortedSetDocValuesField("tag", new BytesRef("t")));
                    writer.addDocument(document);
                }
            }
            try (DirectoryReader firstReader = DirectoryReader.open(first); DirectoryReader secondReader = DirectoryReader.open(second)) {
                final AtomicLong charged = new AtomicLong();
                final CodecReadAttribution attribution = new CodecReadAttribution(
                    ProfileSelection.of(null, Names.EVERY),
                    charged::addAndGet
                );
                final List<LeafReader> leaves = List.of(
                    firstReader.leaves().get(0).reader(),
                    secondReader.leaves().get(0).reader(),
                    firstReader.leaves().get(0).reader()
                );
                long before = 0;
                for (int i = 0; i < leaves.size(); i++) {
                    final LeafReader leaf = leaves.get(i);
                    attribution.leaf(leaf);
                    attribution.partition(i == 1 ? "second:0" : "first:0");
                    attribution.metrics("ColumNAR", Lucene.segmentReader(leaf).getSegmentName(), "tag", Map.of()).counter(READ).add(1);
                    if (i == 1) {
                        assertThat(
                            "the first description that differs is charged",
                            charged.get() - before,
                            equalTo(CodecReadAttribution.AGREED_DESCRIPTION_BYTES)
                        );
                    }
                    before = charged.get();
                }
                assertThat("a later reader adds no charge", charged.get(), equalTo(before));
                final CodecReads.Detail detail = attribution.snapshot().details().get(0);
                assertThat(detail.field(), equalTo("tag"));
                assertThat(detail.reads(), equalTo(counted(3)));
                assertFalse("the indices number the field differently", detail.fieldInfo().containsKey("number"));
                assertThat("and agree on the rest", detail.fieldInfo().get("doc_values_type"), equalTo("SORTED_SET"));
            }
        }
    }

    public void testASelectedPairIsDescribedFromItsLeafAndAnAggregateNever() throws IOException {
        try (Directory directory = newDirectory()) {
            final String codecName;
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                codecName = writer.getConfig().getCodec().getName();
                for (int doc = 0; doc < 10; doc++) {
                    final Document document = new Document();
                    document.add(new SortedSetDocValuesField("tag", new BytesRef("t" + doc)));
                    document.add(new StringField("id", Integer.toString(doc), Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.deleteDocuments(new Term("id", "3"));
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final String segment = Lucene.segmentReader(leaf).getSegmentName();

                final CodecReadAttribution aggregateOnly = new CodecReadAttribution(ProfileSelection.NONE, CodecReadAttribution.NO_CHARGE);
                aggregateOnly.leaf(leaf);
                aggregateOnly.metrics("ColumNAR", segment, "tag", Map.of()).counter(READ).add(1);
                assertThat(aggregateOnly.snapshot().segments(), equalTo(Map.of()));
                assertThat(aggregateOnly.snapshot().details(), equalTo(List.of()));

                final AtomicLong charged = new AtomicLong();
                final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.EVERYTHING, charged::addAndGet);
                attribution.leaf(leaf);
                attribution.metrics("ColumNAR", segment, "tag", Map.of("skip_index", true));
                attribution.metrics("ColumNAR", segment, "id", Map.of());
                attribution.metrics("ColumNAR", segment, "no_such_field", Map.of());
                final CodecReads reads = attribution.snapshot();

                final CodecReads.SegmentContext described = reads.segments().get(segment);
                assertThat(described.maxDoc(), equalTo(10));
                assertThat("the deleted document is not live", described.numDocs(), equalTo(9));
                assertThat(described.fieldInfos().count(), equalTo(2));
                assertThat(described.fieldInfos().docValuesCount(), equalTo(1));
                assertThat(described.luceneVersion(), equalTo(org.apache.lucene.util.Version.LATEST.toString()));
                assertThat(described.luceneCodec(), equalTo(codecName));
                assertThat(reads.segments().keySet(), equalTo(java.util.Set.of(segment)));

                final Map<String, CodecReads.Detail> byField = new java.util.TreeMap<>();
                reads.details().forEach(detail -> byField.put(detail.field(), detail));
                final Map<String, Object> tag = byField.get("tag").fieldInfo();
                assertThat(tag.get("doc_values_type"), equalTo("SORTED_SET"));
                assertThat(tag.get("index_options"), equalTo("NONE"));
                assertThat(tag.get("number"), equalTo(leaf.getFieldInfos().fieldInfo("tag").number));
                assertThat(byField.get("tag").facts(), equalTo(Map.of("skip_index", CodecReads.Fact.TRUE)));
                final Map<String, Object> id = byField.get("id").fieldInfo();
                assertThat(id.get("doc_values_type"), equalTo("NONE"));
                assertThat(id.get("index_options"), equalTo("DOCS"));
                assertThat("the codec did not say", byField.get("id").facts(), equalTo(Map.of()));
                assertThat("a field the segment does not have is not described", byField.get("no_such_field").fieldInfo(), nullValue());

                final long measured = RamUsageTester.ramUsed(described) - RamUsageTester.ramUsed(described.luceneCodec());
                assertThat(
                    "the charge estimates the context " + measured,
                    CodecReadAttribution.bytesOf(described),
                    greaterThanOrEqualTo(measured / 2)
                );
                assertThat(CodecReadAttribution.bytesOf(described), lessThanOrEqualTo(measured * 2));
                assertThat(
                    "three details, the facts of one and the segment, described once",
                    charged.get(),
                    equalTo(
                        3 * (CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES) + CodecReadAttribution.bytesOf(described)
                            + CodecReadAttribution.factBytes(Map.of("skip_index", true))
                    )
                );
            }
        }
    }

    public void testAnIndexSortIsDescribedInTheTermsOfTheSettings() {
        final SortField hostName = new SortedSetSortField("host.name", false, SortedSetSelector.Type.MIN, SortField.STRING_LAST);
        final SortField timestamp = new SortedNumericSortField(
            "@timestamp",
            SortField.Type.LONG,
            true,
            SortedNumericSelector.Type.MAX,
            Long.MIN_VALUE
        );
        assertThat(
            CodecReadAttribution.describeSortKey(hostName),
            equalTo(new CodecReads.SortKey("host.name", "STRING", "asc", "min", "last"))
        );
        assertThat(
            "the smallest value sorts last in a descending sort",
            CodecReadAttribution.describeSortKey(timestamp),
            equalTo(new CodecReads.SortKey("@timestamp", "LONG", "desc", "max", "last"))
        );
        final SortField plain = new SortField("count", SortField.Type.INT);
        assertThat(
            "no missing value is set",
            CodecReadAttribution.describeSortKey(plain),
            equalTo(new CodecReads.SortKey("count", "INT", "asc", null, null))
        );

        for (boolean reverse : new boolean[] { false, true }) {
            assertThat(CodecReadAttribution.missing(Long.MIN_VALUE, "LONG", reverse), equalTo(reverse ? "last" : "first"));
            assertThat(CodecReadAttribution.missing(Long.MAX_VALUE, "LONG", reverse), equalTo(reverse ? "first" : "last"));
            assertThat(CodecReadAttribution.missing(Integer.MIN_VALUE, "INT", reverse), equalTo(reverse ? "last" : "first"));
            assertThat(CodecReadAttribution.missing(Double.POSITIVE_INFINITY, "DOUBLE", reverse), equalTo(reverse ? "first" : "last"));
            assertThat(CodecReadAttribution.missing(-Float.MAX_VALUE, "FLOAT", reverse), equalTo(reverse ? "last" : "first"));
            assertThat(CodecReadAttribution.missing(SortField.STRING_FIRST, "STRING", reverse), equalTo("first"));
        }
        assertThat("a value Elasticsearch never sets is shown as it is", CodecReadAttribution.missing(42L, "LONG", false), equalTo("42"));
        assertThat(CodecReadAttribution.missing(null, "LONG", false), nullValue());
    }

    public void testNestedDocsAreCountedWhenCheapOrAskedFor() throws IOException {
        try (Directory directory = newDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    directory,
                    newIndexWriterConfig().setParentField("__root").setIndexSort(new Sort(new SortField("ts", SortField.Type.LONG)))
                )
            ) {
                for (int block = 0; block < 4; block++) {
                    final List<Document> docs = new ArrayList<>();
                    for (int child = 0; child < block; child++) {
                        docs.add(new Document());
                    }
                    final Document parent = new Document();
                    parent.add(new SortedSetDocValuesField("tag", new BytesRef("t" + block)));
                    parent.add(new NumericDocValuesField("ts", block));
                    docs.add(parent);
                    writer.addDocuments(docs);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final String segment = Lucene.segmentReader(leaf).getSegmentName();
                for (boolean count : new boolean[] { false, true }) {
                    final CodecReadAttribution attribution = new CodecReadAttribution(
                        ProfileSelection.EVERYTHING.withCountNestedDocs(count),
                        CodecReadAttribution.NO_CHARGE
                    );
                    attribution.leaf(leaf);
                    attribution.metrics("ColumNAR", segment, "tag", Map.of());
                    attribution.metrics("ColumNAR", segment, "ts", Map.of());
                    final CodecReads reads = attribution.snapshot();
                    final CodecReads.SegmentContext described = reads.segments().get(segment);
                    assertThat(described.maxDoc(), equalTo(10));
                    assertThat(
                        "counting the blocks reads the parent field, so it is asked for",
                        described.rootDocs(),
                        equalTo(count ? 4 : null)
                    );
                    assertThat(described.fieldInfos().parentField(), equalTo("__root"));
                    assertThat(described.indexSort(), equalTo(List.of(new CodecReads.SortKey("ts", "LONG", "asc", null, null))));
                    final Map<String, Map<String, Object>> fields = new java.util.TreeMap<>();
                    reads.details().forEach(detail -> fields.put(detail.field(), detail.fieldInfo()));
                    assertThat(fields.get("ts").get("sort_position"), equalTo(0));
                    assertThat(fields.get("tag").get("sort_field"), equalTo(false));
                    assertFalse(fields.get("tag").containsKey("sort_position"));
                    if (count) {
                        final Map<String, Object> rendered = new java.util.LinkedHashMap<>();
                        described.render(rendered);
                        assertThat(rendered.get("root_docs"), equalTo(4));
                        assertThat(rendered.get("nested_docs"), equalTo(6));
                    }
                }
            }
        }
    }

    public void testALeafThatIsNotTheNamedSegmentDescribesNothing() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                final Document document = new Document();
                document.add(new SortedSetDocValuesField("tag", new BytesRef("t")));
                writer.addDocument(document);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final CodecReadAttribution stale = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
                stale.leaf(reader.leaves().get(0).reader());
                stale.metrics("ColumNAR", "_not_this_one", "tag", Map.of());
                final CodecReadAttribution unknown = new CodecReadAttribution(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
                unknown.metrics("ColumNAR", "_0", "tag", Map.of());
                for (CodecReads reads : List.of(stale.snapshot(), unknown.snapshot())) {
                    assertThat(reads.segments(), equalTo(Map.of()));
                    assertThat(reads.details().get(0).fieldInfo(), nullValue());
                    assertThat("the pair is reported all the same", reads.details().size(), equalTo(1));
                }
            }
        }
    }

    public void testBindingMakesTheReadersOfThisThreadReportHere() {
        final CodecReadAttribution attribution = new CodecReadAttribution(ProfileSelection.NONE, CodecReadAttribution.NO_CHARGE);
        try (Releasable ignored = attribution.bind()) {
            Scopes.current("ColumNAR", "_1", "f", Map.of()).counter(READ).add(1);
        }
        assertNull(Scopes.current("ColumNAR", "_1", "f", Map.of()));
        assertThat(attribution.snapshot().totals().get("ColumNAR"), equalTo(counted(1)));
    }

    private record Chosen(boolean every, Set<String> chosen) {
        static Chosen randomFrom(List<String> names) {
            return switch (between(0, 2)) {
                case 0 -> new Chosen(false, Set.of());
                case 1 -> new Chosen(true, Set.of());
                case 2 -> new Chosen(false, Set.copyOf(randomSubsetOf(names)));
                default -> throw new AssertionError();
            };
        }

        boolean includes(String name) {
            return every || chosen.contains(name);
        }

        @Nullable
        Names names() {
            return every ? Names.EVERY : chosen.isEmpty() ? null : Names.of(chosen);
        }
    }

    private static String codecOf(String field) {
        return Integer.parseInt(field.substring(1)) % 2 == 0 ? "ColumNAR" : "Other";
    }

    private static CounterMetric readOf(String field) {
        return codecOf(field).equals("ColumNAR") ? READ : OTHER_READ;
    }

    private static Map<String, Long> byCodec(Object metrics) {
        final Map<String, Long> counts = new java.util.TreeMap<>();
        ((Map<?, ?>) metrics).forEach((codec, components) -> counts.put((String) codec, readCount(components)));
        return counts;
    }

    private static long readCount(Object components) {
        return ((Number) ((Map<?, ?>) ((Map<?, ?>) components).get("blocks")).get("read")).longValue();
    }

    private static ProfileSelection segments(String... names) {
        return ProfileSelection.of(Names.of(List.of(names)), null);
    }

    private static ProfileSelection fields(String... names) {
        return ProfileSelection.of(null, Names.of(List.of(names)));
    }

    private static Map<String, Object> blocksRead(long blocks) {
        return Map.of("ColumNAR", Map.of("blocks", Map.of("read", blocks)));
    }

    private static Snapshot counted(long blocks) {
        return Snapshot.of(List.of(new CounterMeasurement(READ, blocks)));
    }

    private static void register(MetricRecorder stats, int counters, int timers) {
        for (int i = 0; i < counters; i++) {
            stats.counter(COUNTERS.get(i));
        }
        for (int i = 0; i < timers; i++) {
            stats.timer(TIMERS.get(i));
        }
    }
}
