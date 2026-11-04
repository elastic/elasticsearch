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
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Numeric fields of a strictly columnar index write their doc values with the ColumNAR codec. Each test indexes the same
 * documents into an index with the codec and one without it, and requires the two to answer everything alike: {@code _source},
 * queries, sorts and aggregations, across flushes and merges.
 */
public class ColumnarNumericCodecTests extends ESSingleNodeTestCase {

    private static final String WITH_CODEC = "with-codec";
    private static final String WITHOUT_CODEC = "without-codec";
    private static final List<String> TYPES = List.of("byte", "short", "integer", "long", "half_float", "float", "double");
    /** Values every type holds exactly, repeated so that a term matches several documents. */
    private static final List<String> VALUES = List.of("-3", "-1", "0", "1", "2", "7", "100", "1.5", "-2.25");

    @Before
    public void assumeNumericCodecFlagEnabled() {
        assumeTrue(
            "columnar_codec_numeric feature flag must be enabled",
            ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.isEnabled()
        );
    }

    public void testAgreesWithoutTheCodec() throws IOException {
        final String type = randomFrom(TYPES);
        final boolean indexed = randomBoolean();
        // multi_value=false writes no offsets companion, and takes only documents holding at most one value.
        final boolean multiValue = randomBoolean();
        final boolean ignoreMalformed = randomBoolean();
        final String nullValue = randomBoolean() ? null : randomFrom(valuesFor(type));
        final String mapping = Strings.format(
            """
                {"properties":{"@timestamp":{"type":"date"},"n":{"type":"%s","index":%s,"multi_value":%s,"ignore_malformed":%s%s}}}""",
            type,
            indexed,
            multiValue,
            ignoreMalformed,
            nullValue == null ? "" : ",\"null_value\":" + nullValue
        );
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(WITH_CODEC, mode, true, mapping);
        createIndex(WITHOUT_CODEC, mode, false, mapping);

        final List<String> sources = randomSources(type, multiValue, ignoreMalformed);
        final int firstBatch = between(1, sources.size());
        indexSources(sources.subList(0, firstBatch), 0);
        refresh();
        indexSources(sources.subList(firstBatch, sources.size()), firstBatch);
        if (randomBoolean()) {
            // Deleting a document makes the merge decode the values rather than copy what each segment wrote.
            for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                client().prepareDelete(index, "0").get();
            }
        }
        refresh();
        if (randomBoolean()) {
            for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
                indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get();
            }
            refresh();
        }

        assertEquals(ColumnarFormat.NAME, docValuesFormat(WITH_CODEC));
        assertNotEquals(ColumnarFormat.NAME, docValuesFormat(WITHOUT_CODEC));

        for (int i = 0; i < sources.size(); i++) {
            final String id = Integer.toString(i);
            assertEquals(
                "_source of [" + id + "] from " + sources.get(i),
                client().prepareGet(WITHOUT_CODEC, id).get().getSourceAsMap(),
                client().prepareGet(WITH_CODEC, id).get().getSourceAsMap()
            );
        }

        for (String value : VALUES) {
            assertSameHits(QueryBuilders.termQuery("n", value));
            assertSameHits(QueryBuilders.rangeQuery("n").gte(value));
            assertSameHits(QueryBuilders.rangeQuery("n").lt(value));
        }
        assertSameHits(QueryBuilders.termsQuery("n", randomSubsetOf(between(1, 3), VALUES)));
        assertSameHits(QueryBuilders.existsQuery("n"));

        for (SortOrder order : SortOrder.values()) {
            assertEquals("sort " + order, sortValues(WITHOUT_CODEC, order), sortValues(WITH_CODEC, order));
        }
        assertEquals(aggregations(WITHOUT_CODEC), aggregations(WITH_CODEC));
        assertEquals(docValueFields(WITHOUT_CODEC), docValueFields(WITH_CODEC));
        final int slices = between(2, 4);
        for (int slice = 0; slice < slices; slice++) {
            assertEquals("slice " + slice, sliceIds(WITHOUT_CODEC, slice, slices), sliceIds(WITH_CODEC, slice, slices));
        }
    }

    /** Each document's values for {@code n} as {@code docvalue_fields} fetches them from the doc values, by id. */
    private Map<String, List<Object>> docValueFields(String index) {
        final Map<String, List<Object>> values = new HashMap<>();
        assertResponse(client().prepareSearch(index).addDocValueField("n").setSize(100), response -> {
            for (SearchHit hit : response.getHits()) {
                values.put(hit.getId(), hit.field("n") == null ? List.of() : hit.field("n").getValues());
            }
        });
        return values;
    }

    /** The ids of the documents in one slice of a scroll sliced on {@code n}, which hashes the field's doc values. */
    private List<String> sliceIds(String index, int slice, int slices) {
        final List<String> ids = new ArrayList<>();
        final SearchRequestBuilder search = client().prepareSearch(index)
            .setScroll(TimeValue.timeValueMinutes(1))
            .slice(new SliceBuilder("n", slice, slices))
            .setSize(100);
        final String[] scrollId = new String[1];
        assertResponse(search, response -> {
            scrollId[0] = response.getScrollId();
            for (SearchHit hit : response.getHits()) {
                ids.add(hit.getId());
            }
        });
        client().prepareClearScroll().addScrollId(scrollId[0]).get();
        ids.sort(null);
        return ids;
    }

    /**
     * A field that sorts the index keeps {@code SORTED_NUMERIC} doc values, which Lucene's index sort reads, while the other
     * numeric fields of the same index use the codec.
     */
    public void testIndexSortFieldKeepsSortedNumeric() throws IOException {
        // half_float can't sort an index, whatever its doc values
        final String type = randomValueOtherThan("half_float", () -> randomFrom(TYPES));
        final String mapping = Strings.format("""
            {"properties":{"sort_by":{"type":"%s"},"n":{"type":"%s"}}}""", type, type);
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR)
            .put("index.sort.field", "sort_by")
            .build();
        indicesAdmin().prepareCreate(WITH_CODEC).setSettings(settings).setMapping(mapping).get();
        for (int i = 0; i < 10; i++) {
            prepareIndex(WITH_CODEC).setId(Integer.toString(i)).setSource("sort_by", 10 - i, "n", i).get();
        }
        indicesAdmin().prepareRefresh(WITH_CODEC).get();

        assertEquals(ColumnarFormat.NAME, docValuesFormat(WITH_CODEC, "n"));
        assertNotEquals(ColumnarFormat.NAME, docValuesFormat(WITH_CODEC, "sort_by"));
        assertResponse(
            client().prepareSearch(WITH_CODEC).setQuery(QueryBuilders.rangeQuery("n").gte(5)).addSort("sort_by", SortOrder.ASC),
            response -> {
                assertEquals(5, response.getHits().getTotalHits().value());
                assertEquals("9", response.getHits().getAt(0).getId());
            }
        );
    }

    /**
     * Collapsing groups documents by the field's single value, which it reads from the doc values whatever layout stores them.
     */
    public void testCollapseAgreesWithoutTheCodec() throws IOException {
        final String type = randomFrom(TYPES);
        final String mapping = Strings.format("""
            {"properties":{"@timestamp":{"type":"date"},"n":{"type":"%s","index":%s}}}""", type, randomBoolean());
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(WITH_CODEC, mode, true, mapping);
        createIndex(WITHOUT_CODEC, mode, false, mapping);

        // Collapsing requires a single value a document, so documents hold one value or none.
        final List<String> values = valuesFor(type);
        final int docs = between(2, 60);
        final List<String> sources = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            final String timestamp = "\"@timestamp\":\"2024-01-01T00:00:" + String.format(Locale.ROOT, "%02d", i) + "Z\"";
            // The last document always has a value, so there are doc values to look at.
            sources.add(i < docs - 1 && randomBoolean() ? "{" + timestamp + "}" : "{" + timestamp + ",\"n\":" + randomFrom(values) + "}");
        }
        indexSources(sources, 0);
        refresh();

        assertEquals(ColumnarFormat.NAME, docValuesFormat(WITH_CODEC));
        assertEquals(collapsedValues(WITHOUT_CODEC), collapsedValues(WITH_CODEC));
    }

    /** The value of each group collapsing on {@code n} returns, in order. */
    private List<String> collapsedValues(String index) {
        final List<String> values = new ArrayList<>();
        final SearchRequestBuilder search = client().prepareSearch(index).setCollapse(new CollapseBuilder("n")).setSize(100);
        assertResponse(search, response -> {
            for (SearchHit hit : response.getHits()) {
                final Object value = hit.field("n") == null ? null : hit.field("n").getValue();
                values.add(String.valueOf(value));
            }
        });
        values.sort(null);
        return values;
    }

    /** The {@link #VALUES} a field of {@code type} holds exactly. */
    private static List<String> valuesFor(String type) {
        return type.endsWith("float") || type.equals("double") ? VALUES : VALUES.stream().filter(v -> v.contains(".") == false).toList();
    }

    /**
     * Random documents for a field of {@code type}: absent, a single value, or, if {@code multiValue}, an array that may repeat
     * values, hold nulls or be empty, which the codec has to reproduce through {@code _source} in order. If
     * {@code ignoreMalformed}, some documents hold a value the field can't parse, which it ignores.
     */
    private List<String> randomSources(String type, boolean multiValue, boolean ignoreMalformed) {
        final List<String> values = valuesFor(type);
        final int docs = between(2, 60);
        final List<String> sources = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            final String timestamp = "\"@timestamp\":\"2024-01-01T00:00:" + String.format(Locale.ROOT, "%02d", i) + "Z\"";
            int shape = between(0, 4);
            if ((shape == 3 && multiValue == false) || (shape == 4 && ignoreMalformed == false)) {
                shape = 1;
            }
            // The last document always has a value, so there are doc values to look at whichever documents are deleted.
            sources.add(switch (i == docs - 1 ? 1 : shape) {
                case 0 -> "{" + timestamp + "}";
                case 1 -> "{" + timestamp + ",\"n\":" + randomFrom(values) + "}";
                case 2 -> "{" + timestamp + ",\"n\":null}";
                case 3 -> {
                    final String[] array = new String[between(0, 5)];
                    for (int j = 0; j < array.length; j++) {
                        array[j] = randomBoolean() && randomBoolean() ? "null" : randomFrom(values);
                    }
                    yield "{" + timestamp + ",\"n\":[" + String.join(",", Arrays.asList(array)) + "]}";
                }
                case 4 -> "{" + timestamp + ",\"n\":\"not a number\"}";
                default -> throw new AssertionError();
            });
        }
        return sources;
    }

    private void createIndex(String index, IndexMode mode, boolean codec, String mapping) {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey(), codec)
            .build();
        indicesAdmin().prepareCreate(index).setSettings(settings).setMapping(mapping).get();
    }

    private void indexSources(List<String> sources, int firstId) {
        if (sources.isEmpty()) {
            return;
        }
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < sources.size(); i++) {
                bulk.add(prepareIndex(index).setId(Integer.toString(firstId + i)).setSource(sources.get(i), XContentType.JSON));
            }
            final var response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
    }

    private void refresh() {
        for (String index : List.of(WITH_CODEC, WITHOUT_CODEC)) {
            assertEquals(0, indicesAdmin().prepareRefresh(index).get().getFailedShards());
        }
    }

    private void assertSameHits(QueryBuilder query) {
        assertEquals(query.toString(), hitIds(WITHOUT_CODEC, query), hitIds(WITH_CODEC, query));
    }

    private List<String> hitIds(String index, QueryBuilder query) {
        final List<String> ids = new ArrayList<>();
        assertResponse(client().prepareSearch(index).setQuery(query).setSize(100), response -> {
            for (SearchHit hit : response.getHits()) {
                ids.add(hit.getId());
            }
        });
        ids.sort(null);
        return ids;
    }

    /** The sort values of every document with a value, in order; ties may come back in either order, but their values can't. */
    private List<Object> sortValues(String index, SortOrder order) {
        final List<Object> values = new ArrayList<>();
        final SearchRequestBuilder search = client().prepareSearch(index)
            .setQuery(QueryBuilders.existsQuery("n"))
            .addSort("n", order)
            .setSize(100);
        assertResponse(search, response -> {
            for (SearchHit hit : response.getHits()) {
                values.add(hit.getSortValues()[0]);
            }
        });
        return values;
    }

    private Map<String, Double> aggregations(String index) {
        final Map<String, Double> results = new HashMap<>();
        final SearchRequestBuilder search = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(AggregationBuilders.min("min").field("n"))
            .addAggregation(AggregationBuilders.max("max").field("n"))
            .addAggregation(AggregationBuilders.sum("sum").field("n"))
            .addAggregation(AggregationBuilders.count("count").field("n"));
        assertResponse(search, response -> {
            results.put("min", ((Min) response.getAggregations().get("min")).value());
            results.put("max", ((Max) response.getAggregations().get("max")).value());
            results.put("sum", ((Sum) response.getAggregations().get("sum")).value());
            results.put("count", (double) ((ValueCount) response.getAggregations().get("count")).getValue());
        });
        return results;
    }

    private String docValuesFormat(String index) throws IOException {
        return docValuesFormat(index, "n");
    }

    /** The doc values format {@code field}'s values were written with in {@code index}. */
    private String docValuesFormat(String index, String field) throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            String format = null;
            boolean found = false;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    final String leafFormat = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                    if (found) {
                        assertEquals("leaves of [" + index + "] disagree on the doc values format", format, leafFormat);
                    }
                    format = leafFormat;
                    found = true;
                }
            }
            assertTrue("expected a document of [" + index + "] with a value for [" + field + "] to assert on", found);
            return format;
        }
    }
}
