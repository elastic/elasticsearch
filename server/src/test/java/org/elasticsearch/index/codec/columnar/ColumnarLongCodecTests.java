/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.columnar.numeric.NumericBinaryValues;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.plain.ColumnarLongIndexFieldData;
import org.elasticsearch.index.fielddata.plain.ColumnarNumericSortField;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.columnar.numeric.NumericBinaryValues.INITIAL_DECODE_CAPACITY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A {@code long} field of a strict columnar index that opts into
 * {@link IndexSettings#BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING} is written with the ColumNAR codec. These tests index the
 * same documents into an index with the setting on and one with it off, which keeps the field on {@code SORTED_NUMERIC}
 * doc values, and require searches against the two to agree.
 */
public class ColumnarLongCodecTests extends ESSingleNodeTestCase {

    private static final String WITH_CODEC = "columnar-long";
    private static final String WITHOUT_CODEC = "sorted-numeric-long";
    private static final String FIELD = "l";

    @Before
    public void requireFeatureFlag() {
        assumeTrue("the ColumNAR numeric codec is behind a feature flag", IndexSettings.BINARY_NUMERIC_DOC_VALUES_FEATURE_FLAG.isEnabled());
    }

    /**
     * Only {@code long} fields are routed to the codec, and they keep array order in their payload, so there is no
     * {@code .offsets} companion.
     */
    public void testOnlyLongFieldsUseTheCodec() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, "type=long", "i", "type=integer");
        createIndex(WITHOUT_CODEC, settings(mode, false), "@timestamp", "type=date", FIELD, "type=long");
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            prepareIndex(index).setSource("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"l\":[3,1,2],\"i\":[3,1,2]}", XContentType.JSON).get();
            indicesAdmin().prepareRefresh(index).get();
        }

        final FieldInfo columnar = fieldInfo(WITH_CODEC, FIELD);
        assertEquals(DocValuesType.BINARY, columnar.getDocValuesType());
        assertEquals(ColumnarFormat.NAME, columnar.getAttribute("PerFieldDocValuesFormat.format"));
        assertNull("no offsets companion", fieldInfo(WITH_CODEC, FieldArrayContext.offsetsFieldName(FIELD)));
        assertEquals(DocValuesType.SORTED_NUMERIC, fieldInfo(WITH_CODEC, "i").getDocValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fieldInfo(WITHOUT_CODEC, FIELD).getDocValuesType());
    }

    /**
     * An indexed field writes points, or terms with {@code index_terms}, beside its payload. A document whose array holds only
     * nulls has nothing to index, and Lucene requires every document to present a field the same way, so it must not present
     * the payload alone either.
     */
    public void testIndexedFieldWithOnlyNullsInAnArray() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final String mapping = "type=long,index=true,index_terms=" + randomBoolean();
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, mapping);
        final List<String> arrays = randomBoolean() ? List.of("[1, 2]", "[null]") : List.of("[null]", "[1, 2]");
        for (int i = 0; i < arrays.size(); i++) {
            prepareIndex(WITH_CODEC).setId(Integer.toString(i))
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"l\":" + arrays.get(i) + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC).get();
        final String withValues = Integer.toString(arrays.indexOf("[1, 2]"));
        assertSameHitsIn(WITH_CODEC, QueryBuilders.rangeQuery(FIELD).gte(1), Set.of(withValues));
        assertSameHitsIn(WITH_CODEC, QueryBuilders.existsQuery(FIELD), Set.of(withValues));
    }

    private void assertSameHitsIn(String index, QueryBuilder query, Set<String> expected) {
        assertEquals(query.toString(), expected, hitIds(index, query));
    }

    /** The setting only means something in a strict columnar mode, and is refused in any other. */
    public void testSettingRejectedOutsideColumnarModes() {
        final IndexMode mode = randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareCreate(WITH_CODEC).setSettings(settings(mode, randomBoolean())).get()
        );
        assertThat(e.getMessage(), containsString(IndexSettings.BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING.getKey()));
    }

    /**
     * Range and term queries, sorting and aggregations over dense, sparse, single- and multi-valued values, across several
     * segments, deletions and a merge, return what they return on {@code SORTED_NUMERIC} doc values.
     */
    public void testSearchesAgreeWithSortedNumeric() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"id":{"type":"keyword"},"l":{"type":"long","index":%s,"index_terms":%s}}}\
            """, indexed, indexed && randomBoolean());
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            indicesAdmin().prepareCreate(index).setSettings(settings(mode, index.equals(WITH_CODEC))).setMapping(mapping).get();
        }

        final boolean singleValued = randomBoolean();
        final boolean sparse = randomBoolean();
        final int numDocs = between(1, 600);
        final Map<String, List<Long>> valuesById = new LinkedHashMap<>();
        for (int d = 0; d < numDocs; d++) {
            valuesById.put(Integer.toString(d), randomValues(singleValued, sparse));
        }
        final List<String> ids = new ArrayList<>(valuesById.keySet());
        for (int from = 0; from < ids.size();) {
            final int to = Math.min(ids.size(), from + between(1, 200));
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (String id : ids.subList(from, to)) {
                for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                    bulk.add(prepareIndex(index).setId(id).setSource(source(id, valuesById.get(id)), XContentType.JSON));
                }
            }
            assertNoFailures(bulk.get());
            indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
            from = to;
        }
        if (randomBoolean()) {
            for (String id : randomSubsetOf(between(0, Math.min(10, ids.size())), ids)) {
                client().prepareDelete(WITH_CODEC, id).get();
                client().prepareDelete(WITHOUT_CODEC, id).get();
                valuesById.remove(id);
            }
        }
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(WITH_CODEC, WITHOUT_CODEC).setMaxNumSegments(1).get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        if (valuesById.values().stream().anyMatch(v -> v != null && v.stream().anyMatch(Objects::nonNull))) {
            assertEquals(ColumnarFormat.NAME, fieldInfo(WITH_CODEC, FIELD).getAttribute("PerFieldDocValuesFormat.format"));
        }

        final List<Long> present = valuesById.values()
            .stream()
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .toList();
        for (int i = 0; i < 20; i++) {
            final long lower = present.isEmpty() || randomBoolean() ? randomLongBetween(-100, 100) : randomFrom(present);
            // An upper bound below the lower one is a range that matches nothing, which both indices have to agree on too.
            final long upper = present.isEmpty() || randomBoolean() ? randomLongBetween(-100, 100) : randomFrom(present);
            assertSameHits(QueryBuilders.rangeQuery(FIELD).from(lower, randomBoolean()).to(upper, randomBoolean()));
        }
        assertSameHits(QueryBuilders.rangeQuery(FIELD).gte(randomLongBetween(-100, 100)));
        assertSameHits(QueryBuilders.rangeQuery(FIELD).lt(randomLongBetween(-100, 100)));
        final long term = present.isEmpty() ? 7L : randomFrom(present);
        assertSameHits(QueryBuilders.termQuery(FIELD, term));
        assertSameHits(QueryBuilders.termsQuery(FIELD, new long[] { term, randomLongBetween(-100, 100) }));
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameSort(SortOrder.ASC);
        assertSameSort(SortOrder.DESC);
        assertSameAggregations();
    }

    /**
     * The payload keeps a document's non-null values in the order they were written, duplicates included, and drops its nulls,
     * so {@code _source} gives back the same arrays as {@code SORTED_NUMERIC} doc values do for the arrays without their nulls.
     */
    public void testSourceKeepsArrayOrder() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = "type=long,index=" + indexed + ",index_terms=" + (indexed && randomBoolean());
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, mapping);
        createIndex(WITHOUT_CODEC, settings(mode, false), "@timestamp", "type=date", FIELD, mapping);
        final Map<String, String> arrays = new LinkedHashMap<>();
        arrays.put("unsorted", "[3, 1, 2]");
        arrays.put("duplicates", "[5, 5, 1, 5]");
        arrays.put("scalar", "4");
        arrays.put("one-element", "[4]");
        arrays.put("with-null", "[2, null, 1]");
        arrays.put("duplicates-around-nulls", "[null, 5, null, 5, 1, null]");
        arrays.put("only-null", "[null]");
        arrays.put("only-nulls", "[null, null]");
        arrays.put("null", "null");
        arrays.put("empty", "[]");
        indexIntoBoth(arrays, true);

        final Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("unsorted", List.of(3, 1, 2));
        expected.put("duplicates", List.of(5, 5, 1, 5));
        expected.put("scalar", 4);
        expected.put("one-element", 4);
        expected.put("with-null", List.of(2, 1));
        expected.put("duplicates-around-nulls", List.of(5, 5, 1));
        final Map<String, Object> withCodec = sourceValues(WITH_CODEC, arrays.size());
        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            assertEquals(entry.getKey(), entry.getValue(), withCodec.get(entry.getKey()));
        }
        for (String nothingButNulls : List.of("only-null", "only-nulls", "null", "empty")) {
            assertNull(nothingButNulls, withCodec.get(nothingButNulls));
        }
        assertEquals(sourceValues(WITHOUT_CODEC, arrays.size()), withCodec);
        // A document holding only nulls has no value: it is missing, and nulls count for nothing.
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameAggregations();
    }

    /**
     * {@code null_value} replaces a null before nulls are dropped, so the replacement is indexed in the null's place and the
     * document matches {@code exists}.
     */
    public void testNullValueReplacesNulls() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = "type=long,null_value=7,index=" + indexed + ",index_terms=" + (indexed && randomBoolean());
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, mapping);
        createIndex(WITHOUT_CODEC, settings(mode, false), "@timestamp", "type=date", FIELD, mapping);
        final Map<String, String> arrays = new LinkedHashMap<>();
        arrays.put("null", "null");
        arrays.put("only-null", "[null]");
        arrays.put("null-first", "[null, 1]");
        arrays.put("null-between", "[2, null, 1]");
        arrays.put("empty", "[]");
        indexIntoBoth(arrays, false);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("null", 7);
        expected.put("only-null", 7);
        expected.put("null-first", List.of(7, 1));
        expected.put("null-between", List.of(2, 7, 1));
        expected.put("empty", null);
        assertEquals(expected, sourceValues(WITH_CODEC, arrays.size()));

        final Set<String> withValues = Set.of("null", "only-null", "null-first", "null-between");
        assertSameHitsIn(WITH_CODEC, QueryBuilders.existsQuery(FIELD), withValues);
        assertSameHitsIn(WITH_CODEC, QueryBuilders.termQuery(FIELD, 7), withValues);
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameHits(QueryBuilders.termQuery(FIELD, 7));
    }

    /**
     * An index sorted by a ColumNAR long keeps its documents in the order {@code SORTED_NUMERIC} doc values would put them in,
     * across flushes and a merge, and searches, sorts and aggregations over it agree with an index sorted the same way on
     * {@code SORTED_NUMERIC} doc values.
     */
    public void testIndexSortedByColumnarLong() throws IOException {
        final SortSpec spec = SortSpec.random();
        createSortedIndices(spec.settings(), longMapping());

        final boolean singleValued = randomBoolean();
        boolean anyValue = false;
        for (int d = between(1, 300); d > 0; d--) {
            final String id = Integer.toString(d);
            final List<Long> values = randomValues(singleValued, true);
            anyValue |= hasValue(values);
            final String source = source(id, values);
            for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                prepareIndex(index).setId(id).setSource(source, XContentType.JSON).get();
            }
            if (rarely()) {
                indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
            }
        }
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(WITH_CODEC, WITHOUT_CODEC).setMaxNumSegments(1).get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        assertSortedLikeSortedNumeric(spec, anyValue);
    }

    /**
     * An index sorted by a ColumNAR long, merged from several segments that hold deleted documents. The flushed segments are
     * sorted from the indexing buffer's payloads, but the merged segment is sorted from the column, and the deletes shift the
     * document ids it is read through, so a wrong merged order would go unnoticed by anything that only looks at one segment.
     */
    public void testIndexSortedByColumnarLongAcrossMergeWithDeletes() throws IOException {
        final SortSpec spec = SortSpec.random();
        createSortedIndices(spec.settings(), longMapping());

        final int numDocs = between(100, 300);
        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            ids.add(Integer.toString(i));
        }
        Collections.shuffle(ids, random());
        final int batches = between(3, 5);
        final int perBatch = (numDocs + batches - 1) / batches;
        for (int from = 0; from < numDocs; from += perBatch) {
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (String id : ids.subList(from, Math.min(from + perBatch, numDocs))) {
                final List<Long> values = switch (Integer.parseInt(id) % 6) {
                    case 0 -> null;
                    case 1 -> List.of();
                    case 2 -> Arrays.asList(randomValue(), null, randomValue());
                    case 3 -> randomList(INITIAL_DECODE_CAPACITY + 1, INITIAL_DECODE_CAPACITY * 2, ColumnarLongCodecTests::randomValue);
                    case 4 -> randomList(2, 5, ColumnarLongCodecTests::randomValue);
                    default -> List.of(randomValue());
                };
                for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                    bulk.add(prepareIndex(index).setId(id).setSource(source(id, values), XContentType.JSON));
                }
            }
            assertNoFailures(bulk.get());
            indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        }
        for (String id : randomSubsetOf(numDocs / 5, ids)) {
            client().prepareDelete(WITH_CODEC, id).get();
            client().prepareDelete(WITHOUT_CODEC, id).get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        indicesAdmin().prepareForceMerge(WITH_CODEC, WITHOUT_CODEC).setMaxNumSegments(1).get();
        indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        assertEquals(1, segmentCount(WITH_CODEC));
        assertSortedLikeSortedNumeric(spec, true);
    }

    /**
     * Keys at either limit of a {@code long}, keys equal to the missing value, and duplicated keys, sorted first from the
     * indexing buffer's payloads and then, merged with a second segment, from the column.
     */
    public void testIndexSortedByColumnarLongAtTheLimits() throws IOException {
        final Map<String, List<Long>> docs = new LinkedHashMap<>();
        docs.put("min", List.of(Long.MIN_VALUE));
        docs.put("min-twice", List.of(Long.MIN_VALUE, Long.MIN_VALUE));
        docs.put("max-and-min", List.of(Long.MAX_VALUE, Long.MIN_VALUE));
        docs.put("zero", List.of(0L));
        docs.put("zeros-and-five", List.of(0L, 0L, 5L));
        docs.put("max", List.of(Long.MAX_VALUE));
        docs.put("missing", null);
        docs.put("empty", List.of());
        final Map<String, List<Long>> late = Map.of("late", List.of(1L, 3L));

        // By minimum, ascending: a missing document takes Long.MAX_VALUE, which ties it with "max".
        final SortSpec minAscending = new SortSpec(false, false, false);
        assertIndexOrder(
            minAscending,
            docs,
            late,
            List.of(Set.of("min", "min-twice", "max-and-min"), Set.of("zero", "zeros-and-five"), Set.of("max", "missing", "empty")),
            List.of(
                Set.of("min", "min-twice", "max-and-min"),
                Set.of("zero", "zeros-and-five"),
                Set.of("late"),
                Set.of("max", "missing", "empty")
            )
        );

        // By maximum, descending, missing first: a missing document again takes Long.MAX_VALUE, which ties it with every
        // document whose maximum is Long.MAX_VALUE.
        indicesAdmin().prepareDelete(WITH_CODEC).get();
        final SortSpec maxDescending = new SortSpec(true, true, true);
        assertIndexOrder(
            maxDescending,
            docs,
            late,
            List.of(Set.of("max-and-min", "max", "missing", "empty"), Set.of("zeros-and-five"), Set.of("zero"), Set.of("min", "min-twice")),
            List.of(
                Set.of("max-and-min", "max", "missing", "empty"),
                Set.of("zeros-and-five"),
                Set.of("late"),
                Set.of("zero"),
                Set.of("min", "min-twice")
            )
        );
    }

    /**
     * Arrays longer than {@link NumericBinaryValues#INITIAL_DECODE_CAPACITY}, sorted first from the indexing buffer's
     * payloads and then, merged with a second segment, from the column. Their minimum and maximum put them in different orders.
     */
    public void testIndexSortedByColumnarLongWithManyValues() throws IOException {
        final Map<String, List<Long>> docs = new LinkedHashMap<>();
        docs.put("a", shuffledList(List.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L)));
        docs.put("b", shuffledList(List.of(5L, 30L, 7L, 8L, 9L, 11L, 12L, 13L, 14L)));
        docs.put("c", List.of(15L));
        final Map<String, List<Long>> late = Map.of("d", shuffledList(List.of(16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L, 24L, 25L)));

        assertIndexOrder(
            new SortSpec(false, false, randomBoolean()),
            docs,
            late,
            List.of(Set.of("b"), Set.of("a"), Set.of("c")),
            List.of(Set.of("b"), Set.of("a"), Set.of("c"), Set.of("d"))
        );
        indicesAdmin().prepareDelete(WITH_CODEC).get();
        assertIndexOrder(
            new SortSpec(false, true, randomBoolean()),
            docs,
            late,
            List.of(Set.of("c"), Set.of("a"), Set.of("b")),
            List.of(Set.of("c"), Set.of("a"), Set.of("d"), Set.of("b"))
        );
    }

    /**
     * Creates {@link #WITH_CODEC} sorted by {@code spec}, indexes {@code docs} into one segment and checks its order, then
     * indexes {@code late} into a second segment, merges the two and checks the order of the merged segment. Each order is a
     * list of groups of ids, in the order the groups sort; ids within one group tie, so their order is not checked.
     */
    private void assertIndexOrder(
        SortSpec spec,
        Map<String, List<Long>> docs,
        Map<String, List<Long>> late,
        List<Set<String>> flushedOrder,
        List<Set<String>> mergedOrder
    ) throws IOException {
        final Settings settings = Settings.builder().put(settings(IndexMode.COLUMNAR, true)).put(spec.settings()).build();
        indicesAdmin().prepareCreate(WITH_CODEC).setSettings(settings).setMapping(longMapping()).get();
        indexInto(WITH_CODEC, docs);
        assertEquals(1, segmentCount(WITH_CODEC));
        assertGroupedOrder(spec + " flushed", flushedOrder, indexOrder(WITH_CODEC));

        indexInto(WITH_CODEC, late);
        indicesAdmin().prepareForceMerge(WITH_CODEC).setMaxNumSegments(1).get();
        indicesAdmin().prepareRefresh(WITH_CODEC).get();
        assertEquals(1, segmentCount(WITH_CODEC));
        assertGroupedOrder(spec + " merged", mergedOrder, indexOrder(WITH_CODEC));
    }

    private void indexInto(String index, Map<String, List<Long>> docs) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (Map.Entry<String, List<Long>> doc : shuffledList(new ArrayList<>(docs.entrySet()))) {
            bulk.add(prepareIndex(index).setId(doc.getKey()).setSource(source(doc.getKey(), doc.getValue()), XContentType.JSON));
        }
        assertNoFailures(bulk.get());
        indicesAdmin().prepareRefresh(index).get();
    }

    /** The ids of the index's documents in index order, which is the order of its only shard's segments. */
    private List<String> indexOrder(String index) {
        final List<String> ids = new ArrayList<>();
        assertResponse(client().prepareSearch(index).addSort("_doc", SortOrder.ASC).setSize(10_000), response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                ids.add(hit.getId());
            }
        });
        return ids;
    }

    private static void assertGroupedOrder(String description, List<Set<String>> expected, List<String> actual) {
        final List<Set<String>> grouped = new ArrayList<>();
        int from = 0;
        for (Set<String> group : expected) {
            final int to = Math.min(actual.size(), from + group.size());
            grouped.add(new HashSet<>(actual.subList(from, to)));
            from = to;
        }
        assertEquals(description + " " + actual, expected, grouped);
        assertEquals(description + " " + actual, from, actual.size());
    }

    /**
     * An index sorted by a ColumNAR long and then by a {@code double}, which keeps its {@link SortedNumericSortField}, agrees
     * with one sorted the same way on {@code SORTED_NUMERIC} doc values, across segments, deletes and a merge.
     */
    public void testIndexSortedByColumnarLongThenDouble() throws IOException {
        assertTwoFieldSortLikeSortedNumeric(true);
    }

    /** As {@link #testIndexSortedByColumnarLongThenDouble}, with the ColumNAR long second. */
    public void testIndexSortedByDoubleThenColumnarLong() throws IOException {
        assertTwoFieldSortLikeSortedNumeric(false);
    }

    private void assertTwoFieldSortLikeSortedNumeric(boolean longFirst) throws IOException {
        final SortSpec longSpec = SortSpec.random();
        final SortSpec doubleSpec = SortSpec.random();
        final List<String> fields = longFirst ? List.of(FIELD, "d") : List.of("d", FIELD);
        final List<SortSpec> specs = longFirst ? List.of(longSpec, doubleSpec) : List.of(doubleSpec, longSpec);
        final Settings sort = Settings.builder()
            .putList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), fields)
            .putList(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), specs.stream().map(s -> s.reverse ? "desc" : "asc").toList())
            .putList(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), specs.stream().map(s -> s.max ? "max" : "min").toList())
            .putList(
                IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(),
                specs.stream().map(s -> s.missingFirst ? "_first" : "_last").toList()
            )
            .build();
        createSortedIndices(sort, """
            {"properties":{"@timestamp":{"type":"date"},"id":{"type":"keyword"},"l":{"type":"long"},"d":{"type":"double"}}}""");

        // Few distinct values, so that documents tie on the first field and the second decides.
        final boolean singleValued = randomBoolean();
        final List<String> ids = new ArrayList<>();
        for (int batch = between(2, 4); batch > 0; batch--) {
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = between(20, 80); i > 0; i--) {
                final String id = Integer.toString(ids.size());
                ids.add(id);
                final StringBuilder source = new StringBuilder("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"id\":\"").append(id).append('"');
                if (randomInt(9) > 0) {
                    final List<Long> values = singleValued
                        ? List.of(randomLongBetween(-3, 3))
                        : randomList(1, 4, () -> randomLongBetween(-3, 3));
                    source.append(",\"l\":").append(values);
                }
                if (randomInt(9) > 0) {
                    source.append(",\"d\":").append(randomFrom(-1.5, 0.0, 2.5, 10.0));
                }
                final String json = source.append('}').toString();
                for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                    bulk.add(prepareIndex(index).setId(id).setSource(json, XContentType.JSON));
                }
            }
            assertNoFailures(bulk.get());
            indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        }
        for (String id : randomSubsetOf(ids.size() / 10, ids)) {
            client().prepareDelete(WITH_CODEC, id).get();
            client().prepareDelete(WITHOUT_CODEC, id).get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        if (randomBoolean()) {
            indicesAdmin().prepareForceMerge(WITH_CODEC, WITHOUT_CODEC).setMaxNumSegments(1).get();
            indicesAdmin().prepareRefresh(WITH_CODEC, WITHOUT_CODEC).get();
        }

        final int longPosition = fields.indexOf(FIELD);
        forEachLeaf(WITH_CODEC, leaf -> {
            final SortField[] sortFields = leaf.reader().getMetaData().sort().getSort();
            assertEquals(longSpec.sortField(), sortFields[longPosition]);
            assertThat(sortFields[1 - longPosition], instanceOf(SortedNumericSortField.class));
        });
        assertEquals(reportedSegmentSorts(WITHOUT_CODEC), reportedSegmentSorts(WITH_CODEC));

        assertSameHits(QueryBuilders.rangeQuery(FIELD).gte(randomLongBetween(-3, 3)));
        assertSameSort(SortOrder.ASC);
        assertSameSort(SortOrder.DESC);
        final int pageSize = between(1, 20);
        final Supplier<List<CompositeValuesSourceBuilder<?>>> sources = () -> fields.stream()
            .<CompositeValuesSourceBuilder<?>>map(
                f -> new TermsValuesSourceBuilder(f).field(f).order(specs.get(fields.indexOf(f)).reverse ? SortOrder.DESC : SortOrder.ASC)
            )
            .toList();
        assertEquals(compositeBuckets(WITHOUT_CODEC, sources, pageSize), compositeBuckets(WITH_CODEC, sources, pageSize));
    }

    /**
     * A segment sorted by each document's minimum isn't in the order of every value it holds: documents {@code [30, 10]} and
     * {@code [20, 12]} sort as 10 then 12, so their values come in the order 10, 30, 12, 20. A composite aggregation that
     * stopped early on the index sort would close its first page of two on 10 and 30. It mustn't, and a document without a
     * value changes nothing.
     */
    public void testCompositeOnMultiValuedIndexSort() throws IOException {
        final SortSpec spec = new SortSpec(false, false, randomBoolean());
        final Settings settings = Settings.builder().put(settings(IndexMode.COLUMNAR, true)).put(spec.settings()).build();
        indicesAdmin().prepareCreate(WITH_CODEC).setSettings(settings).setMapping(longMapping()).get();
        final Map<String, List<Long>> docs = new LinkedHashMap<>();
        docs.put("a", List.of(30L, 10L));
        docs.put("b", List.of(20L, 12L));
        docs.put("missing", null);
        indexInto(WITH_CODEC, docs);

        final Supplier<List<CompositeValuesSourceBuilder<?>>> terms = () -> List.of(new TermsValuesSourceBuilder("l").field(FIELD));
        assertEquals(
            List.of(
                List.of(Map.of("l", 10L), 1L),
                List.of(Map.of("l", 12L), 1L),
                List.of(Map.of("l", 20L), 1L),
                List.of(Map.of("l", 30L), 1L)
            ),
            compositeBuckets(WITH_CODEC, terms, 2)
        );
    }

    /**
     * On a single-valued segment the composite aggregation does stop early on the index sort, and finds the first document
     * after the after key with the sort's own comparator, which has to place a document without a value where the index did.
     */
    public void testCompositeOnSingleValuedIndexSortWithMissing() throws IOException {
        final SortSpec spec = new SortSpec(randomBoolean(), randomBoolean(), randomBoolean());
        final Settings settings = Settings.builder().put(settings(IndexMode.COLUMNAR, true)).put(spec.settings()).build();
        indicesAdmin().prepareCreate(WITH_CODEC).setSettings(settings).setMapping(longMapping()).get();
        final Map<String, List<Long>> docs = new LinkedHashMap<>();
        docs.put("a", List.of(30L));
        docs.put("b", List.of(10L));
        docs.put("c", List.of(20L));
        docs.put("d", List.of(12L));
        docs.put("missing", null);
        indexInto(WITH_CODEC, docs);

        final SortOrder order = spec.reverse ? SortOrder.DESC : SortOrder.ASC;
        final Supplier<List<CompositeValuesSourceBuilder<?>>> terms = () -> List.of(
            new TermsValuesSourceBuilder("l").field(FIELD).order(order)
        );
        final List<Object> ascending = List.of(
            List.of(Map.of("l", 10L), 1L),
            List.of(Map.of("l", 12L), 1L),
            List.of(Map.of("l", 20L), 1L),
            List.of(Map.of("l", 30L), 1L)
        );
        final List<Object> expected = spec.reverse ? ascending.reversed() : ascending;
        for (int pageSize = 1; pageSize <= 4; pageSize++) {
            assertEquals(spec + " page size " + pageSize, expected, compositeBuckets(WITH_CODEC, terms, pageSize));
        }
    }

    /** The order, mode and missing value of one field of an index sort. */
    private record SortSpec(boolean reverse, boolean max, boolean missingFirst) {
        static SortSpec random() {
            return new SortSpec(randomBoolean(), randomBoolean(), randomBoolean());
        }

        /** The index sort settings that sort by {@link #FIELD} alone. */
        Settings settings() {
            return Settings.builder()
                .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), FIELD)
                .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), reverse ? "desc" : "asc")
                .put(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), max ? "max" : "min")
                .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), missingFirst ? "_first" : "_last")
                .build();
        }

        /** The value a document without one sorts as. */
        long missing() {
            return missingFirst ^ reverse ? Long.MIN_VALUE : Long.MAX_VALUE;
        }

        /** The sort field a ColumNAR long sorts the index with. */
        ColumnarNumericSortField sortField() {
            return new ColumnarNumericSortField(FIELD, SortField.Type.LONG, reverse, max, missing());
        }
    }

    private static String longMapping() {
        final boolean indexed = randomBoolean();
        return Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"id":{"type":"keyword"},"l":{"type":"long","index":%s,"index_terms":%s}}}\
            """, indexed, indexed && randomBoolean());
    }

    /** Creates {@link #WITH_CODEC} and {@link #WITHOUT_CODEC} in the same columnar mode with the same index sort. */
    private void createSortedIndices(Settings sort, String mapping) {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            final Settings settings = Settings.builder().put(settings(mode, index.equals(WITH_CODEC))).put(sort).build();
            indicesAdmin().prepareCreate(index).setSettings(settings).setMapping(mapping).get();
        }
    }

    private static boolean hasValue(List<Long> values) {
        return values != null && values.stream().anyMatch(Objects::nonNull);
    }

    /**
     * Each segment of {@link #WITH_CODEC}, sorted by {@link #FIELD} alone as {@code spec} says, records the sort and is in its
     * order; the segments API reports the sort as the {@code SORTED_NUMERIC} index does; and searches, sorts, aggregations and
     * composite pages agree with that index.
     */
    private void assertSortedLikeSortedNumeric(SortSpec spec, boolean anyValue) throws IOException {
        if (anyValue) {
            assertEquals(ColumnarFormat.NAME, fieldInfo(WITH_CODEC, FIELD).getAttribute("PerFieldDocValuesFormat.format"));
        }
        forEachLeaf(WITH_CODEC, leaf -> {
            assertEquals(spec.sortField(), leaf.reader().getMetaData().sort().getSort()[0]);
            assertSegmentSorted(leaf.reader(), spec.reverse, spec.max, spec.missingFirst);
        });
        final Map<String, Object> reported = new LinkedHashMap<>();
        reported.put("field", FIELD);
        reported.put("mode", spec.max ? "max" : "min");
        reported.put("missing", Long.toString(spec.missing()));
        reported.put("reverse", spec.reverse);
        final Set<Object> sorts = reportedSegmentSorts(WITH_CODEC);
        assertEquals(Set.of(List.of(reported)), sorts);
        assertEquals(reportedSegmentSorts(WITHOUT_CODEC), sorts);

        assertSameHits(QueryBuilders.rangeQuery(FIELD).gte(randomLongBetween(-100, 100)));
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameSort(SortOrder.ASC);
        assertSameSort(SortOrder.DESC);
        assertSameAggregations();
        final SortOrder compositeOrder = randomFrom(SortOrder.values());
        final int pageSize = between(1, 50);
        final Supplier<List<CompositeValuesSourceBuilder<?>>> terms = () -> List.of(
            new TermsValuesSourceBuilder("l").field(FIELD).order(compositeOrder)
        );
        assertEquals(compositeBuckets(WITHOUT_CODEC, terms, pageSize), compositeBuckets(WITH_CODEC, terms, pageSize));
        // A rounded source can't use a ColumNAR index sort to terminate early. Its pages are checked against a single page of
        // every bucket rather than the SORTED_NUMERIC index's pages, which can undercount a bucket that spans two pages when
        // they terminate early on the rounded index sort.
        final Supplier<List<CompositeValuesSourceBuilder<?>>> rounded = () -> List.of(
            new DateHistogramValuesSourceBuilder("l").field(FIELD).fixedInterval(new DateHistogramInterval("10ms")).order(compositeOrder)
        );
        assertEquals(compositeBuckets(WITHOUT_CODEC, rounded, 10_000), compositeBuckets(WITH_CODEC, rounded, pageSize));
    }

    /** The index sort each segment of {@code index} reports through the segments API. */
    private Set<Object> reportedSegmentSorts(String index) throws IOException {
        final IndicesSegmentResponse response = indicesAdmin().prepareSegments(index).get();
        final BytesReference rendered = XContentHelper.toXContent(response, XContentType.JSON, false);
        final Map<String, Object> map = XContentHelper.convertToMap(rendered, false, XContentType.JSON).v2();
        final Set<Object> sorts = new HashSet<>();
        for (Object copy : (List<?>) ObjectPath.eval("indices." + index + ".shards.0", map)) {
            for (Object segment : ((Map<?, ?>) ((Map<?, ?>) copy).get("segments")).values()) {
                sorts.add(((Map<?, ?>) segment).get("sort"));
            }
        }
        assertFalse("no segments in [" + index + "]", sorts.isEmpty());
        return sorts;
    }

    private int segmentCount(String index) throws IOException {
        final int[] count = { 0 };
        forEachLeaf(index, leaf -> count[0]++);
        return count[0];
    }

    private void forEachLeaf(String index, CheckedConsumer<LeafReaderContext, IOException> consumer) throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                consumer.accept(leaf);
            }
        }
    }

    /** Each document's sort key, in doc id order, never goes against the index sort. */
    private static void assertSegmentSorted(LeafReader reader, boolean reverse, boolean max, boolean missingFirst) throws IOException {
        final SortedNumericLongValues values = ColumnarLongIndexFieldData.longValues(reader, FIELD);
        Long previous = null;
        boolean seenMissing = false;
        boolean seenValue = false;
        for (int doc = 0; doc < reader.maxDoc(); doc++) {
            if (values.advanceExact(doc) == false) {
                assertFalse("missing documents sort " + (missingFirst ? "first" : "last"), missingFirst ? seenValue : false);
                seenMissing = true;
                continue;
            }
            assertFalse("missing documents sort " + (missingFirst ? "first" : "last"), missingFirst == false && seenMissing);
            seenValue = true;
            long key = values.nextValue();
            for (int i = 1; i < values.docValueCount(); i++) {
                long value = values.nextValue();
                key = max ? Math.max(key, value) : Math.min(key, value);
            }
            if (previous != null) {
                assertTrue("doc " + doc + " sorts after its predecessor", reverse ? key <= previous : key >= previous);
            }
            previous = key;
        }
    }

    /** All buckets of a composite aggregation over the given sources, which can use the index sort to terminate early. */
    private List<Object> compositeBuckets(String index, Supplier<List<CompositeValuesSourceBuilder<?>>> sources, int pageSize) {
        final List<Object> buckets = new ArrayList<>();
        final AtomicReference<Map<String, Object>> after = new AtomicReference<>();
        do {
            final CompositeAggregationBuilder composite = new CompositeAggregationBuilder("composite", sources.get()).size(pageSize);
            if (after.get() != null) {
                composite.aggregateAfter(after.get());
            }
            assertResponse(client().prepareSearch(index).setSize(0).addAggregation(composite), response -> {
                final InternalComposite result = response.getAggregations().get("composite");
                for (InternalComposite.InternalBucket bucket : result.getBuckets()) {
                    buckets.add(Arrays.asList(bucket.getKey(), bucket.getDocCount()));
                }
                after.set(result.getBuckets().isEmpty() ? null : result.afterKey());
            });
        } while (after.get() != null);
        return buckets;
    }

    /**
     * Malformed values of an {@code ignore_malformed} field aren't in the column; synthetic source reads them back from the field's
     * own fallback column, which strict columnar indices write in place of {@code _ignored_source}. So a document's valid values
     * come back first, in the order they were written, and its malformed values after them, as they do without the setting.
     */
    public void testSourceKeepsMalformedValues() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = "type=long,ignore_malformed=true,index=" + indexed + ",index_terms=" + (indexed && randomBoolean());
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, mapping);
        createIndex(WITHOUT_CODEC, settings(mode, false), "@timestamp", "type=date", FIELD, mapping);
        final Map<String, String> arrays = new LinkedHashMap<>();
        arrays.put("malformed", "\"foo\"");
        arrays.put("only-malformed", "[\"foo\", \"bar\"]");
        arrays.put("malformed-among-values", "[3, \"foo\", 1, \"bar\", 3]");
        arrays.put("malformed-among-nulls", "[null, \"foo\", 2, null]");
        arrays.put("malformed-and-null", "[\"foo\", null]");
        arrays.put("values", "[3, 1, 2]");
        indexIntoBoth(arrays, true);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("malformed", "foo");
        expected.put("only-malformed", List.of("foo", "bar"));
        expected.put("malformed-among-values", List.of(3, 1, 3, "foo", "bar"));
        expected.put("malformed-among-nulls", List.of(2, "foo"));
        expected.put("malformed-and-null", "foo");
        expected.put("values", List.of(3, 1, 2));
        final Map<String, Object> withCodec = sourceValues(WITH_CODEC, arrays.size());
        assertEquals(expected, withCodec);
        assertEquals(sourceValues(WITHOUT_CODEC, arrays.size()), withCodec);
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameHits(QueryBuilders.rangeQuery(FIELD).gte(2));
    }

    /**
     * With {@code multi_value: false} and {@code on_failure: ignore}, a document's first value goes into the column and the rest
     * into the field's {@code ._on_failure} column, which synthetic source reads back after it.
     */
    public void testSourceKeepsOnFailureValues() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = "type=long,multi_value=false,on_failure=ignore,index="
            + indexed
            + ",index_terms="
            + (indexed && randomBoolean());
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, mapping);
        createIndex(WITHOUT_CODEC, settings(mode, false), "@timestamp", "type=date", FIELD, mapping);
        final Map<String, String> arrays = new LinkedHashMap<>();
        arrays.put("scalar", "4");
        arrays.put("array", "[3, 1, 2]");
        arrays.put("duplicates", "[5, 5, 1]");
        arrays.put("with-null", "[null, 2, 1]");
        arrays.put("only-null", "[null]");
        // With multi_value: false neither index records offsets, so they treat nulls the same way.
        indexIntoBoth(arrays, false);

        final Map<String, Object> withCodec = sourceValues(WITH_CODEC, arrays.size());
        assertEquals(4, withCodec.get("scalar"));
        assertEquals(List.of(3, 1, 2), withCodec.get("array"));
        assertEquals(sourceValues(WITHOUT_CODEC, arrays.size()), withCodec);
        // Only the first value is in the column, so only it is searchable.
        assertSameHits(QueryBuilders.termQuery(FIELD, 1));
        assertSameHits(QueryBuilders.existsQuery(FIELD));
    }

    /**
     * Indexes each of {@code arrays}, by id, as the field's value in both indices, across a few segments and maybe a merge. With
     * {@code dropNullsWithoutCodec}, the index without the codec is given each array without its nulls, as the ColumNAR column
     * keeps it, since that index's {@code .offsets} companion would put them back.
     */
    private void indexIntoBoth(Map<String, String> arrays, boolean dropNullsWithoutCodec) throws IOException {
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            for (Map.Entry<String, String> entry : shuffledList(new ArrayList<>(arrays.entrySet()))) {
                final String source = dropNullsWithoutCodec && index.equals(WITHOUT_CODEC)
                    ? sourceWithoutNulls(entry.getValue())
                    : "{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"l\":" + entry.getValue() + "}";
                prepareIndex(index).setId(entry.getKey()).setSource(source, XContentType.JSON).get();
                if (rarely()) {
                    indicesAdmin().prepareRefresh(index).get();
                }
            }
            if (randomBoolean()) {
                indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get();
            }
            indicesAdmin().prepareRefresh(index).get();
        }
    }

    /**
     * A document whose field holds {@code json} with its nulls removed: an array loses its null elements, and a bare null leaves
     * the field out.
     */
    private static String sourceWithoutNulls(String json) throws IOException {
        final Object value = XContentHelper.convertToMap(JsonXContent.jsonXContent, "{\"v\":" + json + "}", false).get("v");
        final Map<String, Object> source = new HashMap<>();
        source.put("@timestamp", "2024-01-01T00:00:00Z");
        if (value instanceof List<?> list) {
            source.put(FIELD, list.stream().filter(Objects::nonNull).toList());
        } else if (value != null) {
            source.put(FIELD, value);
        }
        return BytesReference.bytes(JsonXContent.contentBuilder().map(source)).utf8ToString();
    }

    /** The field's value in each document's {@code _source}, by id. */
    private Map<String, Object> sourceValues(String index, int size) {
        final Map<String, Object> values = new HashMap<>();
        assertResponse(client().prepareSearch(index).setSize(size), response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                values.put(hit.getId(), hit.getSourceAsMap().get(FIELD));
            }
        });
        return values;
    }

    private static List<Long> randomValues(boolean singleValued, boolean sparse) {
        if (sparse && randomBoolean()) {
            return null;
        }
        if (singleValued) {
            return List.of(randomValue());
        }
        final int count = between(0, 5);
        final List<Long> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(rarely() ? null : randomValue());
        }
        return values;
    }

    private static long randomValue() {
        return rarely() ? randomLong() : randomLongBetween(-100, 100);
    }

    private static String source(String id, List<Long> values) {
        final StringBuilder source = new StringBuilder("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"id\":\"").append(id).append('"');
        if (values != null) {
            source.append(",\"l\":");
            source.append(values.size() == 1 && values.get(0) != null ? values.get(0).toString() : values.toString());
        }
        return source.append('}').toString();
    }

    private void assertSameHits(QueryBuilder query) {
        assertEquals(query.toString(), hitIds(WITHOUT_CODEC, query), hitIds(WITH_CODEC, query));
    }

    private Set<String> hitIds(String index, QueryBuilder query) {
        final Set<String> ids = new HashSet<>();
        assertResponse(client().prepareSearch(index).setQuery(query).setSize(10_000), response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                ids.add(hit.getId());
            }
        });
        return ids;
    }

    private void assertSameSort(SortOrder order) {
        assertEquals("sorted " + order, sortedIds(WITHOUT_CODEC, order), sortedIds(WITH_CODEC, order));
    }

    private List<String> sortedIds(String index, SortOrder order) {
        final List<String> ids = new ArrayList<>();
        assertResponse(client().prepareSearch(index).addSort(FIELD, order).addSort("id", SortOrder.ASC).setSize(10_000), response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                ids.add(hit.getId());
            }
        });
        return ids;
    }

    private void assertSameAggregations() {
        final List<Object> expected = aggregations(WITHOUT_CODEC);
        final List<Object> actual = aggregations(WITH_CODEC);
        // The sum is a double, which depends on the order the values are added in, and segments and merges can differ between
        // the two indices.
        final double expectedSum = (double) expected.removeFirst();
        assertEquals(expectedSum, (double) actual.removeFirst(), Math.abs(expectedSum) * 1e-12);
        assertEquals(expected, actual);
    }

    private List<Object> aggregations(String index) {
        final List<Object> results = new ArrayList<>();
        final SearchRequestBuilder search = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("sum").field(FIELD))
            .addAggregation(AggregationBuilders.min("min").field(FIELD))
            .addAggregation(AggregationBuilders.max("max").field(FIELD))
            .addAggregation(AggregationBuilders.count("count").field(FIELD))
            .addAggregation(AggregationBuilders.missing("missing").field(FIELD))
            .addAggregation(AggregationBuilders.terms("terms").field(FIELD).size(10_000));
        assertResponse(search, response -> {
            results.add(((Sum) response.getAggregations().get("sum")).value());
            results.add(((Min) response.getAggregations().get("min")).value());
            results.add(((Max) response.getAggregations().get("max")).value());
            results.add(((ValueCount) response.getAggregations().get("count")).getValue());
            results.add(((InternalMissing) response.getAggregations().get("missing")).getDocCount());
            final LongTerms terms = response.getAggregations().get("terms");
            for (LongTerms.Bucket bucket : terms.getBuckets()) {
                results.add(Arrays.asList(bucket.getKey(), bucket.getDocCount()));
            }
        });
        return results;
    }

    private static Settings settings(IndexMode mode, boolean codecEnabled) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexSettings.BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING.getKey(), codecEnabled)
            .build();
    }

    /** The field's info in the index's first segment that has it, or {@code null} if none does. */
    private FieldInfo fieldInfo(String index, String field) throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
                if (fieldInfo != null) {
                    return fieldInfo;
                }
            }
            return null;
        }
    }
}
