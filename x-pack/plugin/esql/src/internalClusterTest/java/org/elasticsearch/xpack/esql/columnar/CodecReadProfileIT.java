/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.Measurement;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.profile.CodecReads;
import org.elasticsearch.search.profile.ProfileSelection;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

import static org.elasticsearch.search.profile.CodecReadProfilingTestUtils.assumeCodecReadProfilingEnabled;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class CodecReadProfileIT extends AbstractEsqlIntegTestCase {

    private static final Metric.Id READERS_OPENED = new Metric.Id("ColumNAR", "readers", "opened");
    private static final int DOCS = 2000;
    private static final int VALUE_LENGTH = 100;
    private static final String QUERY_TAIL = " | STATS rows = COUNT(*) BY kw | STATS groups = COUNT(*)";

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testAProfiledReadOfAColumnarKeywordColumnReportsTheCodecWorkOnlyWithCodecReadProfiling() {
        final String index = "columnar_profile";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword")
        );
        indexUniqueValues(index);
        assertThat("the values were written with the ColumNAR codec", keywordDocValuesFormat(index), equalTo(ColumnarFormat.NAME));

        final Map<String, Long> columnar;
        try (EsqlQueryResponse response = run(profiled("FROM " + index + QUERY_TAIL))) {
            assertThat(response.profile(), notNullValue());
            assertThat(onlyValue(response), equalTo((long) DOCS));
            assertFalse("the operators are profiled", valuesSourceReaderStatuses(response).isEmpty());
            columnar = columnarOf(response);
        }
        if (ProfileSelection.CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled()) {
            // NOTE: Which metrics a read records belongs to the codec and is pinned by its own tests; this checks the wiring.
            assertTrue("the codec counted work for the operator: " + columnar, columnar.values().stream().anyMatch(count -> count > 0));
            assertTrue(
                "the codec timed work for the operator: " + columnar,
                columnar.keySet().stream().anyMatch(key -> key.endsWith("_time_nanos"))
            );
        } else {
            assertThat("without codec read profiling no operator reports codec reads", columnar, equalTo(Map.of()));
        }

        try (EsqlQueryResponse unprofiled = run(syncEsqlQueryRequest("FROM " + index + QUERY_TAIL))) {
            assertThat("profiling does not change the result", onlyValue(unprofiled), equalTo((long) DOCS));
        }
    }

    public void testEachFieldOfAQueryIsAttributedItsOwnWorkAndTheTotalIsTheirSum() {
        assumeCodecReadProfilingEnabled();
        final String index = "columnar_fields_profile";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword", "tag", "type=keyword")
        );
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < DOCS; doc++) {
            bulk.add(
                client().prepareIndex(index)
                    .setSource("kw", "unique-value-" + String.format(Locale.ROOT, "%08d", doc), "tag", "term-" + (doc % 20))
            );
        }
        assertFalse(bulk.get().hasFailures());
        assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());

        final Map<String, Map<String, Long>> byField = new TreeMap<>();
        try (
            EsqlQueryResponse response = run(
                profiled("FROM " + index + " | STATS rows = COUNT(*) BY kw, tag | STATS groups = COUNT(*)", ProfileSelection.EVERYTHING)
            )
        ) {
            assertThat(onlyValue(response), equalTo((long) DOCS));
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                final Map<String, Long> summed = new TreeMap<>();
                for (CodecReads.Detail pair : status.codecReads().details()) {
                    assertThat("a pair is keyed by its shard and its segment", pair.segment(), matchesPattern(index + ":0/_[0-9a-z]+"));
                    assertThat(pair.codec(), equalTo(ColumnarFormat.NAME));
                    assertThat("the field is described from the shard's own segment", pair.fieldInfo(), hasKey("doc_values_type"));
                    assertTrue("ColumNAR says whether the column has a skip index", pair.facts().containsKey("skip_index"));
                    final CodecReads.SegmentContext segment = status.codecReads().segments().get(pair.segment());
                    assertThat("the segment names the index it belongs to", segment.index(), equalTo(index));
                    assertThat(segment.shard(), equalTo(0));
                    assertThat(segment.maxDoc(), equalTo(DOCS));
                    assertThat(segment.numDocs(), equalTo(DOCS));
                    assertThat("a segment without blocks counts its documents for free", segment.rootDocs(), equalTo(DOCS));
                    assertThat(segment.fieldInfos().docValuesFormats().get(ColumnarFormat.NAME), greaterThanOrEqualTo(2));
                    TestCollectors.toKeyedValues(pair.reads())
                        .forEach(
                            (name, value) -> byField.computeIfAbsent(pair.field(), f -> new TreeMap<>()).merge(name, value, Long::sum)
                        );
                    for (Measurement measurement : pair.reads().measurements()) {
                        if (measurement instanceof CounterMeasurement counter) {
                            summed.merge(counter.id().key(), counter.value(), Long::sum);
                        }
                    }
                }
                assertThat("with every pair selected the counters add up to the total", summed, equalTo(counters(status)));
            }
        }
        assertThat(byField.keySet(), equalTo(Set.of("kw", "tag")));
        for (Map.Entry<String, Map<String, Long>> field : byField.entrySet()) {
            assertTrue(
                "each field is attributed work of its own: " + field,
                field.getValue().values().stream().anyMatch(count -> count > 0)
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testEachSelectorExpandsOnlyItsOwnDimension() {
        assumeCodecReadProfilingEnabled();
        final String index = "columnar_many_fields";
        final int fields = between(30, 50);
        final List<String> names = new ArrayList<>();
        final StringBuilder mapping = new StringBuilder("{\"properties\":{");
        for (int field = 0; field < fields; field++) {
            final String name = String.format(Locale.ROOT, "field_%02d", field);
            names.add(name);
            mapping.append(field == 0 ? "" : ",").append('"').append(name).append("\":{\"type\":\"keyword\"}");
        }
        mapping.append("}}");
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping(mapping.toString())
        );
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final int docs = 200;
        for (int doc = 0; doc < docs; doc++) {
            final Map<String, Object> source = new TreeMap<>();
            for (String name : names) {
                source.put(name, name + "-" + doc);
            }
            bulk.add(client().prepareIndex(index).setSource(source));
        }
        assertFalse(bulk.get().hasFailures());
        assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
        final String query = "FROM " + index + " | KEEP " + String.join(", ", names) + " | LIMIT " + docs;
        final List<String> selected = randomSubsetOf(2, names);

        try (EsqlQueryResponse response = run(profiled(query))) {
            final String rendered = renderedStatuses(response);
            assertThat(rendered, containsString("\"totals\":{\"ColumNAR\":{"));
            assertThat(rendered, not(containsString("\"segments\"")));
            for (String name : names) {
                assertThat(rendered, not(containsString("\"" + name + "\"")));
            }
            assertThat(rendered, not(containsString("\"fields\"")));
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                assertThat(status.codecReads().details(), equalTo(List.of()));
            }
        }

        try (EsqlQueryResponse response = run(profiled(query, ProfileSelection.of(Names.EVERY, null)))) {
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                final Map<String, Object> rendered = XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(status), true);
                assertFalse("every segment does not expand the fields", rendered.containsKey("fields"));
                final Map<String, Long> segmentsSum = new TreeMap<>();
                ((Map<String, Object>) rendered.getOrDefault("segments", Map.of())).values().forEach(value -> {
                    final Map<String, Object> segment = (Map<String, Object>) value;
                    assertThat("every segment describes the segments", segment, hasKey("max_doc"));
                    assertThat("every segment does not expand the fields", segment, not(hasKey("fields")));
                    addCounters(segmentsSum, ((Map<String, Object>) segment.get("metrics")).get("ColumNAR"));
                });
                assertThat("the segments partition the work of the operator", segmentsSum, equalTo(nonTimes(counters(status))));
            }
        }

        try (EsqlQueryResponse response = run(profiled(query, ProfileSelection.of(Names.EVERY, Names.of(selected))))) {
            final Set<String> reported = new TreeSet<>();
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                status.codecReads().details().forEach(detail -> {
                    if (detail.field() != null) {
                        reported.add(detail.field());
                    }
                });
            }
            assertThat(reported, equalTo(new TreeSet<>(selected)));
            final String rendered = renderedStatuses(response);
            for (String name : names) {
                assertThat(name, rendered.contains("\"" + name + "\""), equalTo(selected.contains(name)));
            }
        }

        try (EsqlQueryResponse response = run(profiled(query, ProfileSelection.of(null, Names.of(selected))))) {
            final Set<String> reported = new TreeSet<>();
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                final Map<String, Object> rendered = XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(status), true);
                assertFalse("a named field expands no segment", rendered.containsKey("segments"));
                final Map<String, Object> fieldsOf = (Map<String, Object>) rendered.getOrDefault("fields", Map.of());
                reported.addAll(fieldsOf.keySet());
                fieldsOf.forEach(
                    (field, entry) -> assertThat(
                        "a named field reports its totals, nothing more",
                        ((Map<String, Object>) entry).keySet(),
                        equalTo(Set.of("metrics"))
                    )
                );
            }
            assertThat("exactly the selected fields are reported", reported, equalTo(new TreeSet<>(selected)));
        }

        try (EsqlQueryResponse response = run(profiled(query, ProfileSelection.EVERYTHING))) {
            final Set<String> reported = new TreeSet<>();
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                status.codecReads().details().forEach(pair -> reported.add(pair.field()));
            }
            assertThat("every field read is reported, with no cap", reported, equalTo(new TreeSet<>(names)));
        }
    }

    public void testTheSameSegmentNameInTwoIndicesGivesTwoPairs() {
        assumeCodecReadProfilingEnabled();
        final List<String> indices = List.of("columnar_twin_a", "columnar_twin_b");
        for (String index : indices) {
            columnarIndex(index, false);
        }
        final Map<String, Map<String, Long>> readersOpenedByKey = new TreeMap<>();
        try (
            EsqlQueryResponse response = run(
                profiled(
                    "FROM columnar_twin_* | STATS rows = COUNT(*) BY kw | STATS groups = COUNT(*)",
                    ProfileSelection.of(Names.EVERY, Names.of(List.of("kw")))
                )
            )
        ) {
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                for (CodecReads.Detail pair : status.codecReads().details()) {
                    if (pair.segment() == null || pair.field() == null) {
                        continue;
                    }
                    readersOpenedByKey.computeIfAbsent(pair.segment(), s -> new TreeMap<>())
                        .merge(pair.field(), ((CounterMeasurement) pair.reads().get(READERS_OPENED)).value(), Long::sum);
                }
            }
        }
        final Set<String> segmentNames = new TreeSet<>();
        final Set<String> shards = new TreeSet<>();
        for (String key : readersOpenedByKey.keySet()) {
            final int slash = key.lastIndexOf('/');
            shards.add(key.substring(0, slash));
            segmentNames.add(key.substring(slash + 1));
        }
        assertThat("each index's shard has its own pairs", shards, equalTo(Set.of("columnar_twin_a:0", "columnar_twin_b:0")));
        assertThat(readersOpenedByKey.size(), equalTo(2));
        assertThat("both force merged shards name their segment the same", segmentNames.size(), equalTo(1));
    }

    private static String renderedStatuses(EsqlQueryResponse response) {
        final StringBuilder rendered = new StringBuilder();
        for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
            rendered.append(Strings.toString(status));
        }
        return rendered.toString();
    }

    @SuppressWarnings("unchecked")
    private static void addCounters(Map<String, Long> into, Object components) {
        ((Map<String, Object>) components).forEach((component, metrics) -> ((Map<String, Object>) metrics).forEach((name, value) -> {
            if (value instanceof Number count) {
                into.merge("ColumNAR." + component + "." + name, count.longValue(), Long::sum);
            }
        }));
    }

    private static Map<String, Long> nonTimes(Map<String, Long> counts) {
        final Map<String, Long> kept = new TreeMap<>(counts);
        kept.keySet().removeIf(name -> name.endsWith("_time_nanos"));
        return kept;
    }

    public void testConcurrentQueriesEachReportOnlyTheirOwnWork() throws Exception {
        assumeCodecReadProfilingEnabled();
        columnarIndex("concurrent_plain", false);
        columnarIndex("concurrent_dictionary", true);
        final int rounds = between(2, 4);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < rounds; round++) {
                final CountDownLatch start = new CountDownLatch(1);
                final Future<?> plain = executor.submit(() -> assertOnlyItsOwnSegments(start, "concurrent_plain"));
                final Future<?> dictionary = executor.submit(() -> assertOnlyItsOwnSegments(start, "concurrent_dictionary"));
                start.countDown();
                plain.get();
                dictionary.get();
            }
        } finally {
            terminate(executor);
        }
    }

    public void testRepeatedQueriesDoNotAccumulateWhatEarlierOnesCounted() throws InterruptedException {
        assumeCodecReadProfilingEnabled();
        columnarIndex("repeat_plain", false);
        columnarIndex("repeat_dictionary", true);
        final CountDownLatch started = new CountDownLatch(0);
        assertOnlyItsOwnSegments(started, "repeat_plain");
        assertOnlyItsOwnSegments(started, "repeat_dictionary");
        assertOnlyItsOwnSegments(started, "repeat_plain");
    }

    // NOTE: With every segment selected, every reader records into a detail keyed by its own segment. A reader of another
    // query would show as a segment of another index, and the segment counters of each operator must add up to its totals.
    private Void assertOnlyItsOwnSegments(CountDownLatch start, String index) throws InterruptedException {
        start.await();
        try (EsqlQueryResponse response = run(profiled("FROM " + index + QUERY_TAIL, ProfileSelection.of(Names.EVERY, null)))) {
            boolean read = false;
            for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
                final Map<String, Long> segmentsSum = new TreeMap<>();
                for (CodecReads.Detail detail : status.codecReads().details()) {
                    read = true;
                    assertThat("only the segments of the query's own index", detail.segment(), startsWith(index + ":"));
                    for (Measurement measurement : detail.reads().measurements()) {
                        if (measurement instanceof CounterMeasurement counter) {
                            segmentsSum.merge(counter.id().key(), counter.value(), Long::sum);
                        }
                    }
                }
                assertThat("nothing of another query is in the totals", segmentsSum, equalTo(counters(status)));
            }
            assertTrue("the query read a profiled column", read);
        }
        return null;
    }

    private void columnarIndex(String index, boolean repeated) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword")
        );
        if (repeated) {
            indexRepeatedValues(index);
        } else {
            indexUniqueValues(index);
        }
    }

    public void testByteLengthReadsTheLengthsWithoutDeliveringValues() {
        assumeCodecReadProfilingEnabled();
        final String index = "columnar_byte_length_profile";
        columnarIndex(index, false);
        final Map<String, Long> columnar;
        try (EsqlQueryResponse response = run(profiled("FROM " + index + " | EVAL l = BYTE_LENGTH(kw) | STATS total = SUM(l)"))) {
            assertThat(onlyValue(response), equalTo((long) DOCS * VALUE_LENGTH));
            columnar = columnarOf(response);
        }
        assertThat(columnar.get("ColumNAR.pages.byte_lengths_call_time_nanos"), greaterThan(0L));
        assertThat("no value is decoded, so no page is handed over", columnar.get("ColumNAR.pages.as_values"), equalTo(0L));
    }

    public void testAColumnThatIsNotColumnarReportsNoColumnarSection() {
        final String index = "plain_profile";
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("kw", "type=keyword")
        );
        indexUniqueValues(index);
        try (EsqlQueryResponse response = run(profiled("FROM " + index + QUERY_TAIL))) {
            assertThat(response.profile(), notNullValue());
            assertThat(onlyValue(response), equalTo((long) DOCS));
            assertThat(columnarOf(response), equalTo(Map.of()));
        }
    }

    private void indexUniqueValues(String index) {
        indexValues(index, doc -> {
            final String value = "unique-value-".repeat(VALUE_LENGTH / 13 + 1).substring(0, VALUE_LENGTH - 6) + String.format(
                Locale.ROOT,
                "%06d",
                doc
            );
            assertEquals(VALUE_LENGTH, value.length());
            return value;
        });
    }

    private void indexRepeatedValues(String index) {
        indexValues(index, doc -> "term-" + (doc % 20));
    }

    private void indexValues(String index, IntFunction<String> valueOf) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < DOCS; doc++) {
            bulk.add(client().prepareIndex(index).setSource("kw", valueOf.apply(doc)));
        }
        assertFalse(bulk.get().hasFailures());
        assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
    }

    private static EsqlQueryRequest profiled(String query) {
        final EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.profile(true);
        return request;
    }

    private static EsqlQueryRequest profiled(String query, ProfileSelection selection) {
        return profiled(query).profileSelection(selection);
    }

    private static long onlyValue(EsqlQueryResponse response) {
        final Iterator<Object> row = response.values().next();
        final long value = (Long) row.next();
        assertFalse(row.hasNext());
        return value;
    }

    private static Map<String, Long> columnarOf(EsqlQueryResponse response) {
        final Map<String, Long> total = new TreeMap<>();
        for (ValuesSourceReaderOperatorStatus status : valuesSourceReaderStatuses(response)) {
            for (Snapshot snapshot : status.codecReads().totals().values()) {
                TestCollectors.toKeyedValues(snapshot).forEach((name, value) -> total.merge(name, value, Long::sum));
            }
        }
        return total;
    }

    private static Map<String, Long> counters(ValuesSourceReaderOperatorStatus status) {
        final Map<String, Long> counters = new TreeMap<>();
        for (Snapshot snapshot : status.codecReads().totals().values()) {
            for (Measurement measurement : snapshot.measurements()) {
                if (measurement instanceof CounterMeasurement counter) {
                    counters.merge(counter.id().key(), counter.value(), Long::sum);
                }
            }
        }
        return counters;
    }

    private static List<ValuesSourceReaderOperatorStatus> valuesSourceReaderStatuses(EsqlQueryResponse response) {
        final List<ValuesSourceReaderOperatorStatus> statuses = new ArrayList<>();
        for (DriverProfile driver : response.profile().drivers()) {
            for (OperatorStatus operator : driver.operators()) {
                if (operator.status() instanceof ValuesSourceReaderOperatorStatus status) {
                    statuses.add(status);
                }
            }
        }
        return statuses;
    }

    private String keywordDocValuesFormat(String index) {
        final IndexShard shard = internalCluster().getDataNodeInstance(IndicesService.class)
            .indexServiceSafe(resolveIndex(index))
            .getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("columnar-profile-it")) {
            String format = null;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo("kw");
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    format = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                }
            }
            assertNotNull("expected a keyword doc-values field in [" + index + "]", format);
            return format;
        }
    }
}
