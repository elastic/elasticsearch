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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;

/**
 * A {@code long} field of a strict columnar index that opts into
 * {@link IndexSettings#COLUMNAR_NUMERIC_CODEC_ENABLED_SETTING} is written with the ColumNAR codec. These tests index the
 * same documents into an index with the setting on and one with it off, which keeps the field on {@code SORTED_NUMERIC}
 * doc values, and require searches against the two to agree.
 */
public class ColumnarLongCodecTests extends ESSingleNodeTestCase {

    private static final String WITH_CODEC = "columnar-long";
    private static final String WITHOUT_CODEC = "sorted-numeric-long";
    private static final String FIELD = "l";

    @Before
    public void requireFeatureFlag() {
        assumeTrue(
            "the ColumNAR numeric codec is behind a feature flag",
            ColumnarDocValuesFormatSelector.COLUMNAR_NUMERIC_CODEC_FEATURE_FLAG.isEnabled()
        );
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
        assertThat(e.getMessage(), containsString(IndexSettings.COLUMNAR_NUMERIC_CODEC_ENABLED_SETTING.getKey()));
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
    }

    /**
     * An index sorted by a ColumNAR long keeps its documents in the order {@code SORTED_NUMERIC} doc values would put them in,
     * across flushes and a merge, and searches, sorts and aggregations over it agree with an index sorted the same way on
     * {@code SORTED_NUMERIC} doc values.
     */
    public void testIndexSortedByColumnarLong() throws IOException {
        final boolean reverse = randomBoolean();
        final boolean max = randomBoolean();
        final boolean missingFirst = randomBoolean();
        final Settings sort = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), FIELD)
            .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), reverse ? "desc" : "asc")
            .put(IndexSortConfig.INDEX_SORT_MODE_SETTING.getKey(), max ? "max" : "min")
            .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), missingFirst ? "_first" : "_last")
            .build();
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final boolean indexed = randomBoolean();
        final String mapping = Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"id":{"type":"keyword"},"l":{"type":"long","index":%s,"index_terms":%s}}}\
            """, indexed, indexed && randomBoolean());
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            final Settings settings = Settings.builder().put(settings(mode, index.equals(WITH_CODEC))).put(sort).build();
            indicesAdmin().prepareCreate(index).setSettings(settings).setMapping(mapping).get();
        }

        final boolean singleValued = randomBoolean();
        final List<String> ids = new ArrayList<>();
        for (int d = between(1, 300); d > 0; d--) {
            final String id = Integer.toString(d);
            ids.add(id);
            final String source = source(id, randomValues(singleValued, true));
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

        assertEquals(ColumnarFormat.NAME, fieldInfo(WITH_CODEC, FIELD).getAttribute("PerFieldDocValuesFormat.format"));
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(WITH_CODEC)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final SortField sortField = leaf.reader().getMetaData().sort().getSort()[0];
                assertEquals(
                    new ColumnarNumericSortField(
                        FIELD,
                        SortField.Type.LONG,
                        reverse,
                        max,
                        missingFirst ^ reverse ? Long.MIN_VALUE : Long.MAX_VALUE
                    ),
                    sortField
                );
                assertSegmentSorted(leaf.reader(), reverse, max, missingFirst);
            }
        }
        assertNotNull(indicesAdmin().prepareSegments(WITH_CODEC).get());

        assertSameHits(QueryBuilders.rangeQuery(FIELD).gte(randomLongBetween(-100, 100)));
        assertSameHits(QueryBuilders.existsQuery(FIELD));
        assertSameSort(SortOrder.ASC);
        assertSameSort(SortOrder.DESC);
        assertSameAggregations();
        final SortOrder compositeOrder = randomFrom(SortOrder.values());
        final int pageSize = between(1, 50);
        final Supplier<CompositeValuesSourceBuilder<?>> terms = () -> new TermsValuesSourceBuilder("l").field(FIELD).order(compositeOrder);
        assertEquals(compositeBuckets(WITHOUT_CODEC, terms, pageSize), compositeBuckets(WITH_CODEC, terms, pageSize));
        // A rounded source can't use a ColumNAR index sort to terminate early. Its pages are checked against a single page of
        // every bucket rather than the SORTED_NUMERIC index's pages, which can undercount a bucket that spans two pages when
        // they terminate early on the rounded index sort.
        final Supplier<CompositeValuesSourceBuilder<?>> rounded = () -> new DateHistogramValuesSourceBuilder("l").field(FIELD)
            .fixedInterval(new DateHistogramInterval("10ms"))
            .order(compositeOrder);
        assertEquals(compositeBuckets(WITHOUT_CODEC, rounded, 10_000), compositeBuckets(WITH_CODEC, rounded, pageSize));
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

    /** All buckets of a composite aggregation over the field, which can use the index sort to terminate early. */
    private List<Object> compositeBuckets(String index, Supplier<CompositeValuesSourceBuilder<?>> source, int pageSize) {
        final List<Object> buckets = new ArrayList<>();
        final AtomicReference<Map<String, Object>> after = new AtomicReference<>();
        do {
            final CompositeAggregationBuilder composite = new CompositeAggregationBuilder("composite", List.of(source.get())).size(
                pageSize
            );
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
     * own fallback column, which strict columnar indices write in place of {@code _ignored_source}.
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

        final Map<String, Object> withCodec = sourceValues(WITH_CODEC, arrays.size());
        assertEquals("foo", withCodec.get("malformed"));
        assertEquals(List.of(3, 1, 2), withCodec.get("values"));
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
            .addAggregation(AggregationBuilders.terms("terms").field(FIELD).size(10_000));
        assertResponse(search, response -> {
            results.add(((Sum) response.getAggregations().get("sum")).value());
            results.add(((Min) response.getAggregations().get("min")).value());
            results.add(((Max) response.getAggregations().get("max")).value());
            results.add(((ValueCount) response.getAggregations().get("count")).getValue());
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
            .put(IndexSettings.COLUMNAR_NUMERIC_CODEC_ENABLED_SETTING.getKey(), codecEnabled)
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
