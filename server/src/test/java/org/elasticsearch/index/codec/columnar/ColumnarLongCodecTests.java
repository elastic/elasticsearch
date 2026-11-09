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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    /** Only {@code long} fields are routed to the codec, and they keep array order in their payload rather than in {@code .offsets}. */
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
     * An indexed field writes points beside its payload. A document whose array holds only nulls has no point to write, and
     * Lucene requires every document to present a field the same way, so it must not present the payload alone either.
     */
    public void testIndexedFieldWithOnlyNullsInAnArray() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, "type=long,index=true");
        final List<String> arrays = randomBoolean() ? List.of("[1, 2]", "[null]") : List.of("[null]", "[1, 2]");
        for (int i = 0; i < arrays.size(); i++) {
            prepareIndex(WITH_CODEC).setId(Integer.toString(i))
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"l\":" + arrays.get(i) + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC).get();
        assertSameHitsIn(WITH_CODEC, QueryBuilders.rangeQuery(FIELD).gte(1), Set.of(Integer.toString(arrays.indexOf("[1, 2]"))));
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
        final String mapping = Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"id":{"type":"keyword"},"l":{"type":"long","index":%s}}}""", randomBoolean());
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
     * The payload keeps a document's array in the order it was written, duplicates included, which {@code _source} gives back.
     * The column doesn't keep nulls yet: a null is dropped from the array, and an array of nothing but nulls, or an empty one,
     * reads back as the field missing.
     */
    // TODO: expect null slots and empty arrays back once the column keeps them.
    public void testSourceKeepsArrayOrder() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(WITH_CODEC, settings(mode, true), "@timestamp", "type=date", FIELD, "type=long");
        final Map<String, String> arrays = new LinkedHashMap<>();
        arrays.put("unsorted", "[3, 1, 2]");
        arrays.put("duplicates", "[5, 5, 1, 5]");
        arrays.put("scalar", "4");
        arrays.put("one-element", "[4]");
        arrays.put("with-null", "[2, null, 1]");
        arrays.put("only-null", "[null]");
        arrays.put("empty", "[]");
        for (Map.Entry<String, String> entry : arrays.entrySet()) {
            prepareIndex(WITH_CODEC).setId(entry.getKey())
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:00Z\",\"l\":" + entry.getValue() + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC).get();

        final Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("unsorted", List.of(3, 1, 2));
        expected.put("duplicates", List.of(5, 5, 1, 5));
        expected.put("scalar", 4);
        expected.put("one-element", 4);
        expected.put("with-null", List.of(2, 1));
        expected.put("only-null", null);
        expected.put("empty", null);
        assertResponse(client().prepareSearch(WITH_CODEC).setSize(arrays.size()), response -> {
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals(hit.getId(), expected.get(hit.getId()), hit.getSourceAsMap().get(FIELD));
            }
        });
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
        assertEquals(aggregations(WITHOUT_CODEC), aggregations(WITH_CODEC));
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
