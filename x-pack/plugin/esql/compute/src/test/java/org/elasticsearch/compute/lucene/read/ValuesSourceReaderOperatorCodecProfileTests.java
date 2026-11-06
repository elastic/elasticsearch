/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.profile.CodecReadAttribution;
import org.elasticsearch.search.profile.CodecReads;
import org.elasticsearch.search.profile.ProfileSelection;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class ValuesSourceReaderOperatorCodecProfileTests extends ComputeTestCase {

    private static final String CODEC = "ColumNAR";

    private static final CounterMetric STUB_READERS = CounterMetric.of(
        "ColumNAR",
        "test",
        "stub_readers",
        Metric.Unit.COUNT,
        "stub readers built"
    );
    private static final CounterMetric STUB_VALUES = CounterMetric.of(
        "ColumNAR",
        "test",
        "stub_values",
        Metric.Unit.COUNT,
        "values the stubs read"
    );
    private static final TimerMetric STUB_TIME = TimerMetric.of(
        "ColumNAR",
        "test",
        "stub_read_time_nanos",
        TimingPolicy.EXACT,
        "time the stubs spent reading"
    );
    private static final CounterMetric STUB_SOURCE = CounterMetric.of(
        "ColumNAR",
        "test",
        "stub_source",
        Metric.Unit.COUNT,
        "source loaders built"
    );

    private final List<Directory> directories = new ArrayList<>();
    private final List<DirectoryReader> readers = new ArrayList<>();

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(directories);
        assertFalse("a test left a scope bound on its thread", TestCollectors.isBound());
    }

    public void testReadersOpenedWhileLoadingReportToTheProfile() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), true, ProfileSelection.NONE);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 10);
            run(operator, driverContext, 10, 25);
            assertThat(total(operator), equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 25L)));
            assertThat(status(operator).codecReads().details(), equalTo(List.of()));
        }
        assertThat(loader.readersWithACollector.get(), equalTo(1));
        assertThat(loader.readersWithoutACollector.get(), equalTo(0));
    }

    public void testNothingIsCollectedWhenTheQueryIsNotProfiled() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), true, null);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 30);
            assertTrue(status(operator).codecReads().isEmpty());
        }
        assertThat("no scope was bound for the reader", loader.readersWithoutACollector.get(), equalTo(1));
        assertThat(loader.readersWithACollector.get(), equalTo(0));
    }

    public void testWithoutASelectionEveryFieldRecordsIntoTheOneAggregate() throws IOException {
        final int fields = between(2, 6);
        final int docs = between(1, 100);
        final Map<String, StubLoader> aggregateOnly = stubs(fields);
        final Map<String, StubLoader> everyPair = stubs(fields);
        final boolean reuse = randomBoolean();
        final DriverContext driverContext = driverContext();
        try (
            Operator withoutPairs = factory(aggregateOnly, reuse, ProfileSelection.NONE).get(driverContext);
            Operator withPairs = factory(everyPair, reuse, ProfileSelection.EVERYTHING).get(driverContext)
        ) {
            run(withoutPairs, driverContext, 0, docs);
            run(withPairs, driverContext, 0, docs);
            assertThat(total(withoutPairs).get("ColumNAR.test.stub_values"), equalTo((long) fields * docs));
            assertThat("the selection never changes the total", total(withoutPairs), equalTo(total(withPairs)));
            final String rendered = Strings.toString(status(withoutPairs));
            assertTrue(rendered, rendered.contains("\"totals\":{\"ColumNAR\":{"));
            assertFalse("no pair was asked for: " + rendered, rendered.contains("\"segments\""));
            assertThat(status(withPairs).codecReads().details(), hasSize(fields));
        }
        assertThat("every field reports to the operator's one aggregate", collectorsOf(aggregateOnly), hasSize(1));
        assertThat("every pair has a detail of its own", collectorsOf(everyPair), hasSize(fields));
    }

    public void testOnlyTheSelectedPairsAreReportedAndTheTotalCountsEveryField() throws IOException {
        final int fields = between(3, 8);
        final int docs = between(1, 100);
        final Map<String, StubLoader> loaders = stubs(fields);
        final List<String> names = new ArrayList<>(loaders.keySet());
        final Set<String> selected = Set.copyOf(randomSubsetOf(between(1, fields - 1), names));
        final boolean segmentNamed = randomBoolean();
        final ProfileSelection selection = ProfileSelection.of(
            segmentNamed ? Names.of(List.of(randomFrom(segment(), SHARD + "/" + segment()))) : null,
            Names.of(selected)
        );
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory(loaders, randomBoolean(), selection).get(driverContext)) {
            run(operator, driverContext, 0, docs);
            final Map<String, Map<String, Long>> pairs = pairs(operator);
            final Map<String, Map<String, Long>> expected = new TreeMap<>();
            final String segmentKey = segmentNamed ? SHARD + "/" + segment() : null;
            for (String name : selected) {
                expected.put(segmentKey + "/" + name, Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", (long) docs));
            }
            if (segmentNamed) {
                final long rest = fields - selected.size();
                expected.put(segmentKey + "/null", Map.of("ColumNAR.test.stub_readers", rest, "ColumNAR.test.stub_values", rest * docs));
            }
            assertThat(pairs, equalTo(expected));
            assertThat(total(operator).get("ColumNAR.test.stub_values"), equalTo((long) fields * docs));
            assertThat(total(operator).get("ColumNAR.test.stub_readers"), equalTo((long) fields));
        }
        final Set<MetricRecorder> unselected = Collections.newSetFromMap(new IdentityHashMap<>());
        for (String name : names) {
            if (selected.contains(name)) {
                assertThat(loaders.get(name).collectorsSeen, hasSize(1));
            } else {
                unselected.addAll(loaders.get(name).collectorsSeen);
            }
        }
        assertThat("the unselected fields share the aggregate, or the named segment's detail", unselected, hasSize(1));
    }

    public void testANameIsMatchedExactlyAndANameNoPairHasSelectsNothing() throws IOException {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        loaders.put("host.name", new StubLoader());
        loaders.put("message", new StubLoader());
        final ProfileSelection selection = randomFrom(
            ProfileSelection.of(null, Names.of(randomSubsetOf(between(1, 4), "host", "host.*", "*", "_all", "Message", "no_such_field"))),
            ProfileSelection.of(Names.of(List.of("_no_such_segment", SHARD + "/_no_such_segment", "other-index:0/" + segment())), null)
        );
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory(loaders, randomBoolean(), selection).get(driverContext)) {
            run(operator, driverContext, 0, 20);
            assertThat(pairs(operator), equalTo(Map.of()));
            assertThat(total(operator).get("ColumNAR.test.stub_values"), equalTo(40L));
        }
    }

    public void testTheSameSegmentNameInTwoShardsGivesTwoPairs() throws IOException {
        final Map<String, StubLoader> loaders = stubs(1);
        final DriverContext driverContext = driverContext();
        final List<String> shards = List.of("index-a:0", "index-b:0");
        try (Operator operator = factory(loaders, randomBoolean(), ProfileSelection.EVERYTHING, shards).get(driverContext)) {
            final int first = between(1, 40);
            final int second = between(1, 40);
            try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(driverContext.blockFactory(), first + second)) {
                for (int doc = 0; doc < first; doc++) {
                    builder.append(0, 0, doc);
                }
                for (int doc = 0; doc < second; doc++) {
                    builder.append(1, 0, doc);
                }
                operator.addInput(new Page(builder.build(DocVector.config()).asBlock()));
            }
            Releasables.close(operator.getOutput());
            assertThat(
                "both shards name their only segment the same",
                segmentName(readers.get(0).leaves().get(0)),
                equalTo(segmentName(readers.get(1).leaves().get(0)))
            );
            assertThat(
                pairs(operator),
                equalTo(
                    Map.of(
                        "index-a:0/" + segment() + "/field_0",
                        Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", (long) first),
                        "index-b:0/" + segment() + "/field_0",
                        Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", (long) second)
                    )
                )
            );
            assertThat(total(operator).get("ColumNAR.test.stub_values"), equalTo((long) first + second));
            final CodecReads reads = status(operator).codecReads();
            assertThat(
                "each shard's segment is described on its own",
                reads.segments().keySet(),
                equalTo(Set.of("index-a:0/" + segment(), "index-b:0/" + segment()))
            );
            for (CodecReads.SegmentContext described : reads.segments().values()) {
                assertThat(described.maxDoc(), equalTo(100));
                assertThat(described.numDocs(), equalTo(100));
            }
            for (CodecReads.Detail detail : reads.details()) {
                assertNull("a field the segment does not have is not described", detail.fieldInfo());
            }
        }
    }

    public void testOperatorsWithDifferentSelectionsRunningConcurrentlyKeepTheirOwnPairs() throws Exception {
        final int operators = between(2, 6);
        final List<ProfileSelection> selections = new ArrayList<>();
        final List<Map<String, StubLoader>> loaders = new ArrayList<>();
        for (int i = 0; i < operators; i++) {
            selections.add(
                randomFrom(
                    ProfileSelection.NONE,
                    ProfileSelection.EVERYTHING,
                    ProfileSelection.of(null, Names.of(List.of("field_" + between(0, 2))))
                )
            );
            loaders.add(stubs(3));
        }
        final List<ValuesSourceReaderOperator.Factory> factories = new ArrayList<>();
        for (int i = 0; i < operators; i++) {
            factories.add(factory(loaders.get(i), true, selections.get(i)));
        }
        final List<ValuesSourceReaderOperatorStatus> statuses = Collections.synchronizedList(new ArrayList<>());
        final CyclicBarrier start = new CyclicBarrier(operators);
        final List<Thread> threads = new ArrayList<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < operators; i++) {
            final int index = i;
            threads.add(new Thread(() -> {
                try {
                    final DriverContext driverContext = driverContext();
                    try (Operator operator = factories.get(index).get(driverContext)) {
                        start.await();
                        for (int page = 0; page < 10; page++) {
                            run(operator, driverContext, page * 10, page * 10 + 10);
                        }
                        statuses.add(status(operator));
                        final Set<String> expected = new HashSet<>();
                        for (String name : List.of("field_0", "field_1", "field_2")) {
                            final ProfileSelection selection = selections.get(index);
                            if (selection.selectsSegment(segment(), SHARD + "/" + segment())) {
                                expected.add(SHARD + "/" + segment() + "/" + name);
                            } else if (selection.selectsField(name)) {
                                expected.add("null/" + name);
                            }
                        }
                        assertThat(pairs(operator).keySet(), equalTo(expected));
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertThat(statuses, hasSize(operators));
        for (ValuesSourceReaderOperatorStatus status : statuses) {
            assertThat(status.codecReads().totals().get(CODEC).get(STUB_VALUES).value(), equalTo(300L));
        }
    }

    public void testNothingStaysBoundBetweenPages() throws IOException {
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", new StubLoader()), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            assertFalse(TestCollectors.isBound());
            run(operator, driverContext, 0, 10);
            assertFalse(TestCollectors.isBound());
            run(operator, driverContext, 10, 20);
            assertFalse(TestCollectors.isBound());
        }
    }

    public void testEveryReadOpensAReaderWhenReadersAreNotReused() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), false, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        final int pages = between(2, 5);
        try (Operator operator = factory.get(driverContext)) {
            for (int page = 0; page < pages; page++) {
                run(operator, driverContext, page * 10, page * 10 + 10);
            }
            final Map<String, Long> expected = Map.of("ColumNAR.test.stub_readers", (long) pages, "ColumNAR.test.stub_values", pages * 10L);
            assertThat(total(operator), equalTo(expected));
            assertThat(
                "the reader built for every read reports to its pair",
                pairs(operator),
                equalTo(Map.of(SHARD + "/" + segment() + "/stub", expected))
            );
            assertThat("one detail serves every reader of the pair", loader.collectorsSeen, hasSize(1));
        }
        assertThat(loader.readersWithoutACollector.get(), equalTo(0));
    }

    public void testWorkDoneBeforeAFailureIsKeptAndNothingStaysBound() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 12);
            loader.failNextRead = true;
            operator.addInput(page(driverContext, 12, 20));
            expectThrows(IllegalStateException.class, operator::getOutput);
            assertFalse("the scope was restored when the read threw", TestCollectors.isBound());
            assertThat(total(operator), equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 12L)));
        }
    }

    public void testAStatusDoesNotChangeWhenTheOperatorKeepsWorking() throws IOException {
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", new StubLoader()), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 10);
            final ValuesSourceReaderOperatorStatus before = status(operator);
            final CodecReads readsBefore = before.codecReads();
            final String renderedBefore = Strings.toString(before);
            run(operator, driverContext, 10, 40);
            assertThat(before.codecReads(), equalTo(readsBefore));
            assertThat(Strings.toString(before), equalTo(renderedBefore));
            assertThat(total(operator).get("ColumNAR.test.stub_values"), equalTo(40L));
        }
    }

    public void testTwoOperatorsKeepSeparateProfiles() throws IOException {
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", new StubLoader()), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator first = factory.get(driverContext); Operator second = factory.get(driverContext)) {
            run(first, driverContext, 0, 10);
            run(second, driverContext, 0, 7);
            run(first, driverContext, 10, 30);
            assertThat(total(first), equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 30L)));
            assertThat(total(second), equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 7L)));
        }
    }

    public void testWithEveryPairSelectedThePairsAddUpToTheTotal() throws IOException {
        final Map<String, StubLoader> loaders = stubs(between(2, 6));
        loaders.put("from_source", new StubLoader().readingByRowAndNeedingSource());
        final ValuesSourceReaderOperator.Factory factory = factory(loaders, randomBoolean(), ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 10);
            run(operator, driverContext, 10, 30);
            final Map<String, Long> summed = new TreeMap<>();
            pairs(operator).values().forEach(pair -> pair.forEach((key, value) -> summed.merge(key, value, Long::sum)));
            assertThat(summed, equalTo(total(operator)));
        }
    }

    public void testASelectedPairWhoseReaderRegisteredNothingIsStillReported() throws IOException {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        loaders.put("codec_read", new StubLoader());
        loaders.put("registers_nothing", new StubLoader().registeringNothing());
        final ValuesSourceReaderOperator.Factory factory = factory(loaders, true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 20);
            assertThat(pairs(operator).get(SHARD + "/" + segment() + "/registers_nothing"), equalTo(Map.of()));
        }
    }

    public void testAProfiledOperatorOverFieldsWithoutCodecProfilingReportsNoCodecReads() throws IOException {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        final int fields = between(1, 4);
        for (int field = 0; field < fields; field++) {
            loaders.put("plain_" + field, new StubLoader().notACodecReader());
        }
        final ValuesSourceReaderOperator.Factory factory = factory(loaders, randomBoolean(), ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, between(10, 40));
            assertTrue(status(operator).codecReads().isEmpty());
            final String rendered = Strings.toString(status(operator));
            assertFalse(rendered, rendered.contains("\"totals\""));
        }
        for (StubLoader loader : loaders.values()) {
            assertThat(loader.readersWithACollector.get(), equalTo(0));
            assertThat(loader.readersWithoutACollector.get(), greaterThan(0));
        }
    }

    public void testASourceLoadersReaderIsAttributedToTheFieldItReads() throws IOException {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        loaders.put("message", new StubLoader());
        loaders.put("from_source", new StubLoader().readingByRowAndNeedingSource());
        final ValuesSourceReaderOperator.Factory factory = factory(loaders, true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 20);
            final Map<String, Map<String, Long>> pairs = pairs(operator);
            assertThat(
                "the source loader reads _source, so its reader is _source's whichever field asked for it",
                pairs.get(SHARD + "/" + segment() + "/_source"),
                equalTo(Map.of("ColumNAR.test.stub_source", 1L))
            );
            assertThat(
                pairs.keySet(),
                equalTo(
                    Set.of(
                        SHARD + "/" + segment() + "/_source",
                        SHARD + "/" + segment() + "/message",
                        SHARD + "/" + segment() + "/from_source"
                    )
                )
            );
            assertThat(total(operator).get("ColumNAR.test.stub_source"), equalTo(1L));
        }
    }

    @SuppressWarnings("unchecked")
    public void testATimerRendersAsAnObjectInTheTotalAndInItsPair() throws IOException {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        loaders.put("timed", new StubLoader().timingItsReads());
        loaders.put("untimed", new StubLoader());
        final ValuesSourceReaderOperator.Factory factory = factory(loaders, true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator operator = factory.get(driverContext)) {
            run(operator, driverContext, 0, 10);
            run(operator, driverContext, 10, 30);
            final Map<String, Object> rendered = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                Strings.toString(status(operator)),
                true
            );
            final Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) rendered.get("totals")).values()
                .iterator()
                .next();
            final Map<String, Object> timer = (Map<String, Object>) ((Map<String, Object>) total.get("test")).get("stub_read_time_nanos");
            assertThat(timer.keySet(), equalTo(Set.of("value", "unit", "estimated", "total_calls")));
            assertThat(timer.get("estimated"), equalTo(false));
            assertThat(((Number) timer.get("value")).longValue(), greaterThanOrEqualTo(2L));
            final Map<String, Object> fields = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) rendered.get("segments"))
                .get(SHARD + "/" + segment())).get("fields");
            final Map<String, Object> timed = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) fields.get("timed")).get(
                "metrics"
            )).get(CODEC);
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) timed.get("test")).get("stub_read_time_nanos")).get("value"),
                equalTo(timer.get("value"))
            );
            final Map<String, Object> untimed = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) fields.get("untimed"))
                .get("metrics")).get(CODEC);
            assertThat(
                "a pair that does not time its reads has no timer",
                (Map<String, Object>) untimed.get("test"),
                not(hasKey("stub_read_time_nanos"))
            );
            assertThat(((Map<String, Object>) total.get("test")).get("stub_values"), equalTo(60));
        }
    }

    public void testThePairsAreChargedToTheDriversBreakerAndReleasedOnClose() throws IOException {
        final int fields = between(1, 5);
        final DriverContext driverContext = driverContext();
        final long before = driverContext.breaker().getUsed();
        try (Operator operator = factory(stubs(fields), true, ProfileSelection.EVERYTHING).get(driverContext)) {
            run(operator, driverContext, 0, 10);
            final CodecReads reads = status(operator).codecReads();
            assertThat("one segment, described once", reads.segments().size(), equalTo(1));
            assertThat(
                driverContext.breaker().getUsed() - before,
                equalTo(
                    fields * (CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES + 2 * TestCollectors.COUNTER_BYTES)
                        + CodecReadAttribution.bytesOf(reads.segments().values().iterator().next())
                )
            );
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(before));
    }

    public void testATrippedBreakerFailsTheReadAndIsReleasedOnClose() throws IOException {
        final BlockFactory blockFactory = blockFactory(ByteSizeValue.ofBytes(between(2_000, 20_000)));
        final DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (Operator operator = factory(stubs(500), true, ProfileSelection.EVERYTHING).get(driverContext)) {
            operator.addInput(page(driverContext, 0, 1));
            expectThrows(CircuitBreakingException.class, operator::getOutput);
            assertFalse(TestCollectors.isBound());
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testNoScopeIsBoundWhileTheOperatorOnlyHoldsItsInput() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        assertFalse(TestCollectors.isBound());
        try (Operator operator = factory.get(driverContext)) {
            assertFalse("building the operator binds nothing", TestCollectors.isBound());
            operator.addInput(page(driverContext, 0, 10));
            assertFalse("receiving a page binds nothing", TestCollectors.isBound());
            assertThat("no reader is built before the page is read", loader.readersWithACollector.get(), equalTo(0));
            Releasables.close(operator.getOutput());
            assertFalse(TestCollectors.isBound());
            assertThat(loader.readersWithACollector.get(), equalTo(1));
        }
    }

    public void testNoReadIsLostOrGivenToAnotherOwnerWhateverTheReadersDo() throws IOException {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), randomBoolean(), ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        long docsRead = 0;
        try (Operator operator = factory.get(driverContext)) {
            for (int page = 0; page < between(3, 8); page++) {
                final int from = between(0, 80);
                final int to = from + between(1, 20);
                run(operator, driverContext, from, to);
                docsRead += to - from;
            }
            assertThat("every doc read was counted once", total(operator).get("ColumNAR.test.stub_values"), equalTo(docsRead));
            assertThat(
                "every reader built reported to the operator",
                total(operator).get("ColumNAR.test.stub_readers"),
                equalTo((long) loader.readersWithACollector.get())
            );
            assertThat(loader.readersWithoutACollector.get(), equalTo(0));
            assertThat(pairs(operator).get(SHARD + "/" + segment() + "/stub").get("ColumNAR.test.stub_values"), equalTo(docsRead));
        }
    }

    public void testAnOperatorMovedToAnotherThreadReopensItsReaderUnderTheSameProfile() throws Exception {
        final StubLoader loader = new StubLoader();
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", loader), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        final ExecutorService first = Executors.newSingleThreadExecutor();
        final ExecutorService second = Executors.newSingleThreadExecutor();
        try (Operator operator = factory.get(driverContext)) {
            first.submit(() -> run(operator, driverContext, 0, 3)).get();
            assertFalse("the first worker keeps no scope bound", first.submit(TestCollectors::isBound).get());
            final ValuesSourceReaderOperatorStatus afterFirst = status(operator);

            second.submit(() -> run(operator, driverContext, 3, 8)).get();
            assertFalse("the second worker keeps no scope bound", second.submit(TestCollectors::isBound).get());

            final Map<String, Long> expected = Map.of("ColumNAR.test.stub_readers", 2L, "ColumNAR.test.stub_values", 8L);
            assertThat("the second worker opened its own reader for the same operator", total(operator), equalTo(expected));
            assertThat(pairs(operator), equalTo(Map.of(SHARD + "/" + segment() + "/stub", expected)));
            assertThat(
                "a status taken before the move does not change",
                TestCollectors.toKeyedValues(afterFirst.codecReads().totals().get(CODEC)),
                equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 3L))
            );
            assertThat("one detail serves the readers of both workers", loader.collectorsSeen, hasSize(1));
        } finally {
            terminate(first, second);
        }
        assertThat(loader.readersWithoutACollector.get(), equalTo(0));
    }

    public void testAnOperatorBuiltAfterAnotherOneClosedStartsFromNothing() throws IOException {
        final ValuesSourceReaderOperator.Factory factory = factory(Map.of("stub", new StubLoader()), true, ProfileSelection.EVERYTHING);
        final DriverContext driverContext = driverContext();
        try (Operator first = factory.get(driverContext)) {
            run(first, driverContext, 0, 40);
        }
        try (Operator second = factory.get(driverContext)) {
            assertTrue(status(second).codecReads().isEmpty());
            run(second, driverContext, 0, 5);
            assertThat(total(second), equalTo(Map.of("ColumNAR.test.stub_readers", 1L, "ColumNAR.test.stub_values", 5L)));
        }
    }

    private static final String SHARD = "test:0";

    private static Map<String, StubLoader> stubs(int fields) {
        final Map<String, StubLoader> loaders = new LinkedHashMap<>();
        for (int field = 0; field < fields; field++) {
            loaders.put("field_" + field, new StubLoader());
        }
        return loaders;
    }

    private static Set<MetricRecorder> collectorsOf(Map<String, StubLoader> loaders) {
        final Set<MetricRecorder> collectors = Collections.newSetFromMap(new IdentityHashMap<>());
        loaders.values().forEach(loader -> collectors.addAll(loader.collectorsSeen));
        return collectors;
    }

    private ValuesSourceReaderOperator.Factory factory(
        Map<String, StubLoader> loaders,
        boolean reuseColumnLoaders,
        @Nullable ProfileSelection profile
    ) throws IOException {
        return factory(loaders, reuseColumnLoaders, profile, List.of(SHARD));
    }

    private ValuesSourceReaderOperator.Factory factory(
        Map<String, StubLoader> loaders,
        boolean reuseColumnLoaders,
        @Nullable ProfileSelection profile,
        List<String> shardIdentifiers
    ) throws IOException {
        ensureIndices(shardIdentifiers.size());
        final List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>();
        loaders.forEach((name, loader) -> {
            loader.field = name;
            fields.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    name,
                    ElementType.INT,
                    false,
                    (ctx, shardIdx) -> ValuesSourceReaderOperator.load(loader)
                )
            );
        });
        final List<ValuesSourceReaderOperator.ShardContext> shards = new ArrayList<>();
        for (int shard = 0; shard < shardIdentifiers.size(); shard++) {
            final DirectoryReader reader = readers.get(shard);
            shards.add(new ValuesSourceReaderOperator.ShardContext(reader, sourcePaths -> {
                final MetricRecorder stats = Scopes.current(CODEC, segmentName(reader.leaves().get(0)), "_source", Map.of());
                if (stats != null) {
                    stats.counter(STUB_SOURCE).add(1);
                }
                return SourceLoader.FROM_STORED_SOURCE;
            }, ValuesSourceReaderOperatorTests.STORED_FIELDS_SEQUENTIAL_PROPORTIONS, shardIdentifiers.get(shard)));
        }
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            fields,
            new IndexedByShardIdFromList<>(shards),
            reuseColumnLoaders,
            0,
            1.0,
            500,
            () -> 0L,
            profile
        );
    }

    private void ensureIndices(int count) throws IOException {
        while (readers.size() < count) {
            final Directory directory = newDirectory();
            directories.add(directory);
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int doc = 0; doc < 100; doc++) {
                    writer.addDocument(new Document());
                }
                writer.commit();
            }
            readers.add(DirectoryReader.open(directory));
        }
    }

    private String segment() throws IOException {
        ensureIndices(1);
        return segmentName(readers.get(0).leaves().get(0));
    }

    private static String segmentName(LeafReaderContext context) {
        return ((SegmentReader) FilterLeafReader.unwrap(context.reader())).getSegmentName();
    }

    private DriverContext driverContext() {
        final BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private static Page page(DriverContext driverContext, int fromDoc, int toDoc) {
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(driverContext.blockFactory(), toDoc - fromDoc)) {
            for (int doc = fromDoc; doc < toDoc; doc++) {
                builder.append(0, 0, doc);
            }
            return new Page(builder.build(DocVector.config()).asBlock());
        }
    }

    private static void run(Operator operator, DriverContext driverContext, int fromDoc, int toDoc) {
        operator.addInput(page(driverContext, fromDoc, toDoc));
        final Page out = operator.getOutput();
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(toDoc - fromDoc));
        Releasables.close(out);
    }

    private static ValuesSourceReaderOperatorStatus status(Operator operator) {
        return (ValuesSourceReaderOperatorStatus) operator.status();
    }

    private static Map<String, Long> total(Operator operator) {
        final Map<String, Long> total = new TreeMap<>();
        return TestCollectors.toKeyedValues(status(operator).codecReads().totals().getOrDefault(CODEC, Snapshot.EMPTY));
    }

    private static Map<String, Map<String, Long>> pairs(Operator operator) {
        final Map<String, Map<String, Long>> pairs = new TreeMap<>();
        for (CodecReads.Detail detail : status(operator).codecReads().details()) {
            assertThat(detail.codec(), equalTo(CODEC));
            pairs.put(detail.segment() + "/" + detail.field(), TestCollectors.toKeyedValues(detail.reads()));
        }
        return pairs;
    }

    // NOTE: Like a codec reader, a stub reader takes its collector when it is built and records later work into it.
    private static final class StubLoader implements BlockLoader {
        final AtomicInteger readersWithACollector = new AtomicInteger();
        final AtomicInteger readersWithoutACollector = new AtomicInteger();
        final Set<MetricRecorder> collectorsSeen = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
        volatile boolean failNextRead;
        private boolean registers = true;
        private boolean timed;
        final AtomicInteger timedReads = new AtomicInteger();
        private boolean rowByRowNeedingSource;
        private boolean consultsTheScope = true;
        String field = "stub";

        StubLoader registeringNothing() {
            this.registers = false;
            return this;
        }

        StubLoader notACodecReader() {
            this.consultsTheScope = false;
            return this;
        }

        StubLoader timingItsReads() {
            this.timed = true;
            return this;
        }

        StubLoader readingByRowAndNeedingSource() {
            this.rowByRowNeedingSource = true;
            return this;
        }

        private StubReader newReader(LeafReaderContext context) {
            final MetricRecorder stats = consultsTheScope ? Scopes.current(CODEC, segmentName(context), field, Map.of()) : null;
            if (stats == null) {
                readersWithoutACollector.incrementAndGet();
                return new StubReader(null, null, this);
            }
            readersWithACollector.incrementAndGet();
            collectorsSeen.add(stats);
            if (registers == false) {
                return new StubReader(null, null, this);
            }
            stats.counter(STUB_READERS).add(1);
            return new StubReader(stats.counter(STUB_VALUES), timed ? stats.timer(STUB_TIME) : null, this);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.ints(expectedCount);
        }

        @Override
        public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
            if (rowByRowNeedingSource) {
                return null;
            }
            return breaker -> newReader(context);
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return newReader(context);
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return rowByRowNeedingSource ? new StoredFieldsSpec(true, false, Set.of()) : StoredFieldsSpec.NO_REQUIREMENTS;
        }

        @Override
        public boolean supportsOrdinals() {
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "stub_loader";
        }
    }

    // NOTE: Like Lucene's doc values readers, a stub reader is used only on the thread that built it.
    private static final class StubReader implements BlockLoader.ColumnAtATimeReader, BlockLoader.RowStrideReader {
        private final Counter values;
        private final Timer time;
        private final StubLoader loader;
        private final Thread creator = Thread.currentThread();

        StubReader(Counter values, Timer time, StubLoader loader) {
            this.values = values;
            this.time = time;
            this.loader = loader;
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered) {
            assertSame("a stub reader is read on the thread that built it", creator, Thread.currentThread());
            if (time != null) {
                loader.timedReads.incrementAndGet();
                time.start();
            }
            try {
                return readPage(factory, docs, offset);
            } finally {
                if (time != null) {
                    time.stop();
                }
            }
        }

        private BlockLoader.Block readPage(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) {
            if (loader.failNextRead) {
                loader.failNextRead = false;
                throw new IllegalStateException("the stub was told to fail");
            }
            final int count = docs.count() - offset;
            if (values != null) {
                values.add(count);
            }
            try (BlockLoader.IntBuilder builder = factory.ints(count)) {
                for (int p = offset; p < docs.count(); p++) {
                    builder.appendInt(docs.get(p));
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) {
            assertSame("a stub reader is read on the thread that built it", creator, Thread.currentThread());
            if (values != null) {
                values.add(1);
            }
            ((BlockLoader.IntBuilder) builder).appendInt(docId);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return Thread.currentThread() == creator;
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return "stub_reader";
        }
    }
}
