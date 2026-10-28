/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LogsdbSortConfigIT extends ESSingleNodeTestCase {

    /**
     * Every test runs in both modes rather than one picked at random, so each run checks the default sort of {@code logsdb_columnar},
     * whose keyword sort fields go through binary doc values, as well as {@code logsdb}.
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { "logsdb" }, new Object[] { "logsdb_columnar" });
    }

    private final String indexMode;

    public LogsdbSortConfigIT(@Name("indexMode") String indexMode) {
        this.indexMode = indexMode;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InternalSettingsPlugin.class, XPackPlugin.class, LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("cluster.logsdb.enabled", "true")
            .put("cluster.logsdb_columnar.enabled", randomBoolean())
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    private record DocWithId(String id, String source) {
        public String source() {
            return source.replace("%id%", id);
        }
    }

    private int id = 0;

    private DocWithId doc(String source) {
        return new DocWithId(Integer.toString(id++), source);
    }

    public void testHostnameMessageTimestampSortConfig() throws IOException {
        final String dataStreamName = "test-logsdb-sort-hostname-message-timestamp";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "message": {
                    "type": "pattern_text"
                  },
                  "test_id": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"aaa\",\"message\":\"bar 5\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":[\"aaa\",\"bbb\"],\"message\":\"bar 7\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":\"aaa\",\"message\":\"bar 9\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"aaa\",\"message\":\"foo 6\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":\"aaa\",\"message\":\"foo 1\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":[\"aaa\",\"bbb\"],\"message\":\"foo 9\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":[\"aaa\",\"bbb\"],\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":[\"aaa\",\"bbb\"],\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":[\"aaa\",\"bbb\"],\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"bbb\",\"message\":\"bar 4\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":\"bbb\",\"message\":\"bar 5\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":\"bbb\",\"message\":\"bar 2\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"bbb\",\"message\":\"foo 7\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":\"bbb\",\"message\":\"foo 3\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":\"bbb\",\"message\":\"foo 6\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"bbb\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"host.name\":\"bbb\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"host.name\":\"bbb\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"message\":\"bar 4\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"message\":\"bar 1\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"message\":\"bar 4\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"message\":\"foo 1\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"message\":\"foo 9\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"message\":\"foo 3\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T12:00:00\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T11:00:00\",\"test_id\": \"%id%\"}") };

        createDataStream(dataStreamName, mapping, b -> b.put("index.logsdb.default_sort_on_message_template", true));

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);

        Index backingIndex = getBackingIndex(dataStreamName);

        var featureService = getInstanceFromNode(FeatureService.class);
        if (featureService.getNodeFeatures().containsKey("mapper.provide_index_sort_setting_defaults")) {
            assertSettings(backingIndex, settings -> {
                assertThat(
                    IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings),
                    equalTo(List.of("host.name", "message.template_id", "@timestamp"))
                );
                assertThat(
                    IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings),
                    equalTo(List.of(SortOrder.ASC, SortOrder.ASC, SortOrder.DESC))
                );
                assertThat(
                    IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings),
                    equalTo(List.of(MultiValueMode.MIN, MultiValueMode.MIN, MultiValueMode.MAX))
                );
                assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of("_last", "_last", "_last")));
            });
        }

        assertOrder(backingIndex, orderedDocs);
    }

    public void testHostnameTimestampSortConfig() throws Exception {
        final String dataStreamName = "test-logsdb-sort-hostname-timestamp";

        final String MAPPING = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\": \"2025-01-01T15:00:00\", \"host.name\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T14:00:00\", \"host.name\": [\"aaa\", \"bbb\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:30:00\", \"host.name\": [\"aaa\", \"bbb\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:00:00\", \"host.name\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:00:00\", \"host.name\": \"bbb\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:00:00\", \"host.name\": \"bbb\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T16:00:00\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:00:00\", \"test_id\": \"%id%\"}") };

        createDataStream(dataStreamName, MAPPING);

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);

        Index backingIndex = getBackingIndex(dataStreamName);

        var featureService = getInstanceFromNode(FeatureService.class);
        if (featureService.getNodeFeatures().containsKey("mapper.provide_index_sort_setting_defaults")) {
            assertSettings(backingIndex, settings -> {
                assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings), equalTo(List.of("host.name", "@timestamp")));
                assertThat(IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings), equalTo(List.of(SortOrder.ASC, SortOrder.DESC)));
                assertThat(IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings), equalTo(List.of(MultiValueMode.MIN, MultiValueMode.MAX)));
                assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of("_last", "_last")));
            });
        }

        assertOrder(backingIndex, orderedDocs);

        SearchRequest searchRequest = new SearchRequest(dataStreamName);
        searchRequest.source().sort("@timestamp", SortOrder.DESC).query(new TermQueryBuilder("host.name", "aaa"));
        var response = client().search(searchRequest).get();
        assertEquals(4, response.getHits().getHits().length);
        response.decRef();
    }

    public void testTimestampOnlySortConfig() throws IOException {
        final String dataStreamName = "test-logsdb-sort-timestamp-only";

        final String MAPPING = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\": \"2025-01-01T15:00:00\", \"host\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T14:00:00\", \"host\": [\"aaa\", \"bbb\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:30:00\", \"host\": [\"aaa\", \"bbb\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:15:00\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:00:00\", \"host\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:45:00\", \"host\": \"bbb\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:15:00\", \"host\": \"bbb\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:00:00\", \"test_id\": \"%id%\"}") };

        createDataStream(dataStreamName, MAPPING);

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);

        Index backingIndex = getBackingIndex(dataStreamName);

        var featureService = getInstanceFromNode(FeatureService.class);
        if (featureService.getNodeFeatures().containsKey("mapper.provide_index_sort_setting_defaults")) {
            assertSettings(backingIndex, settings -> {
                if (IndexSettings.MODE.get(settings) == IndexMode.LOGSDB_COLUMNAR) {
                    // logsdb_columnar defaults to [subobjects:false], allowing [host.name] injection when [host] is a mapped field
                    assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings), equalTo(List.of("host.name", "@timestamp")));
                    assertThat(IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings), equalTo(List.of(SortOrder.ASC, SortOrder.DESC)));
                    assertThat(
                        IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings),
                        equalTo(List.of(MultiValueMode.MIN, MultiValueMode.MAX))
                    );
                    assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of("_last", "_last")));
                } else {
                    // logsdb defaults to [subobjects:true], so [host.name] can't be injected when [host] is a mapped field
                    assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings), equalTo(List.of("@timestamp")));
                    assertThat(IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings), equalTo(List.of(SortOrder.DESC)));
                    assertThat(IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings), equalTo(List.of(MultiValueMode.MAX)));
                    assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of("_last")));
                }
            });
        }

        assertOrder(backingIndex, orderedDocs);
    }

    /**
     * A custom sort on a multi-valued keyword then {@code @timestamp} descending, replacing the defaults. Documents tie on the keyword's
     * minimum so the order within each group is decided by the timestamp, and documents without the keyword sort last.
     */
    public void testCustomKeywordTimestampSortConfig() throws IOException {
        final String dataStreamName = "test-logsdb-sort-custom-keyword-timestamp";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "service": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\": \"2025-01-01T13:00:00\", \"service\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T12:00:00\", \"service\": [\"bbb\", \"aaa\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T11:00:00\", \"service\": \"aaa\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T15:00:00\", \"service\": \"bbb\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T14:00:00\", \"service\": [\"ccc\", \"bbb\"], \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T16:00:00\", \"service\": \"ccc\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T17:00:00\", \"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\": \"2025-01-01T10:00:00\", \"test_id\": \"%id%\"}") };

        createDataStream(
            dataStreamName,
            mapping,
            s -> s.putList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "service", "@timestamp")
                .putList(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "asc", "desc")
        );

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);

        assertOrder(getBackingIndex(dataStreamName), orderedDocs);
    }

    /**
     * A top-N search that does not track total hits and is sorted like the index stops reading each segment once it holds the top N.
     * That needs the query phase to build a sort comparing equal to the segments' index sort and a collector that is allowed to stop.
     * The search runs through the search API with profiling on, whose {@code next_doc_count} is how many documents the query iterated;
     * the same search asking for every document, which cannot stop early, has to iterate them all, and both are checked against a sort
     * computed here. They run over the segments as flushed and again once merged into one, and have to return the same either way.
     */
    public void testTopNTerminatesEarlyWhenSortedLikeTheIndex() throws IOException {
        final String dataStreamName = "test-logsdb-sort-early-termination";
        final boolean descending = randomBoolean();
        final SortOrder order = descending ? SortOrder.DESC : SortOrder.ASC;

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "service": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;
        createDataStream(
            dataStreamName,
            mapping,
            s -> s.put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "service")
                .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), order.toString())
        );

        // One segment per batch, each holding several times the top N, so a segment that stops early leaves most of itself unread.
        final int size = between(1, 5);
        final List<String> keys = new ArrayList<>();
        for (int batch = between(2, 4); batch > 0; batch--) {
            final BulkRequest bulk = new BulkRequest(dataStreamName);
            for (int i = between(size * 4, size * 10); i > 0; i--) {
                final List<String> values = randomServiceValues();
                final Map<String, Object> source = new LinkedHashMap<>();
                source.put("@timestamp", "2025-01-01T00:00:00Z");
                if (values != null) {
                    source.put("service", values);
                }
                bulk.add(new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE).source(source));
                // An ascending sort reads each document's smallest value and a descending one its largest.
                keys.add(values == null ? null : extreme(values, descending));
            }
            assertFalse(client().bulk(bulk).actionGet().hasFailures());
            indicesAdmin().prepareRefresh(dataStreamName).get();
        }
        final Comparator<String> byKey = descending ? Comparator.<String>reverseOrder() : Comparator.<String>naturalOrder();
        keys.sort(Comparator.nullsLast(byKey));

        // The same searches over the segments as flushed and over the one segment merged from them: each segment of either is sorted
        // by the index sort, so both stop early and both return the same documents in the same order.
        final Index backingIndex = getBackingIndex(dataStreamName);
        final List<List<String>> beforeMerge = assertSortedSearches(dataStreamName, backingIndex, order, size, keys);
        indicesAdmin().forceMerge(new ForceMergeRequest(backingIndex.getName()).maxNumSegments(1).flush(true)).actionGet();
        indicesAdmin().prepareRefresh(dataStreamName).get();
        assertThat(segmentCount(backingIndex), equalTo(1));
        final List<List<String>> afterMerge = assertSortedSearches(dataStreamName, backingIndex, order, size, keys);
        assertEquals("order=" + order + " size=" + size + ": the same results before and after merging", beforeMerge, afterMerge);
    }

    /**
     * Runs the top-{@code size} search of {@link #testTopNTerminatesEarlyWhenSortedLikeTheIndex} and the search for every document,
     * checking how many documents each reads and that each returns the first of {@code keys}, and returns the sort keys of both.
     */
    private List<List<String>> assertSortedSearches(
        String dataStreamName,
        Index backingIndex,
        SortOrder order,
        int size,
        List<String> keys
    ) {
        final int segments = segmentCount(backingIndex);
        final String description = "order=" + order + " size=" + size + " docs=" + keys.size() + " segments=" + segments;

        final var untracked = client().prepareSearch(dataStreamName)
            .setSize(size)
            .setTrackTotalHits(false)
            .addSort(SortBuilders.fieldSort("service").order(order))
            .setProfile(true)
            .get();
        final List<String> topKeys;
        try {
            // Each segment reads at most the top N and the one document past it that it stops on.
            final long iterated = documentsIterated(untracked);
            assertThat(description, iterated, lessThanOrEqualTo((long) segments * (size + 1)));
            assertThat(description, iterated, lessThan((long) keys.size()));
            topKeys = sortKeys(untracked);
            assertEquals(description, keys.subList(0, size), topKeys);
        } finally {
            untracked.decRef();
        }

        // The same search asking for every document can stop nowhere, so it pins what a full read counts as. Tracking total hits
        // would not do: a match-all query counts its hits without reading them, which leaves the collector free to stop anyway.
        final var everything = client().prepareSearch(dataStreamName)
            .setSize(keys.size())
            .setTrackTotalHits(false)
            .addSort(SortBuilders.fieldSort("service").order(order))
            .setProfile(true)
            .get();
        final List<String> allKeys;
        try {
            assertThat(
                "a search for every document reads them all, " + description,
                documentsIterated(everything),
                greaterThanOrEqualTo((long) keys.size())
            );
            allKeys = sortKeys(everything);
            assertEquals(description, keys, allKeys);
        } finally {
            everything.decRef();
        }
        return List.of(topKeys, allKeys);
    }

    private int segmentCount(Index backingIndex) {
        final IndexService indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(backingIndex);
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            return searcher.getLeafContexts().size();
        }
    }

    /** The {@code service} sort value of each hit, in the order the search returned them. */
    private static List<String> sortKeys(SearchResponse response) {
        final List<String> sortKeys = new ArrayList<>();
        for (var hit : response.getHits().getHits()) {
            sortKeys.add((String) hit.getSortValues()[0]);
        }
        return sortKeys;
    }

    /** How many documents the profiled query iterated on the search's one shard, from the {@code next_doc_count} of its breakdown. */
    private static long documentsIterated(SearchResponse response) {
        final var shards = response.getSearchProfileShardResults();
        assertThat(shards.values(), hasSize(1));
        long iterated = 0;
        for (var queryProfile : shards.values().iterator().next().getQueryProfileResults()) {
            for (var query : queryProfile.getQueryResults()) {
                iterated += query.getTimeBreakdown().get("next_doc_count");
            }
        }
        return iterated;
    }

    /** The {@code service} values of one document: absent, only a null, one value, or several in no order with a null among them. */
    private List<String> randomServiceValues() {
        return switch (between(0, 3)) {
            case 0 -> null;
            case 1 -> Arrays.asList((String) null);
            case 2 -> List.of("svc-" + between(0, 30));
            default -> {
                final List<String> values = new ArrayList<>();
                for (int v = between(2, 4); v > 0; v--) {
                    values.add(randomBoolean() || values.isEmpty() ? "svc-" + between(0, 30) : null);
                }
                yield values;
            }
        };
    }

    /** The largest non-null value when {@code max}, else the smallest, or {@code null} where there is none. */
    private static String extreme(List<String> values, boolean max) {
        return values.stream()
            .filter(v -> v != null)
            .reduce((a, b) -> (max ? a.compareTo(b) >= 0 : a.compareTo(b) <= 0) ? a : b)
            .orElse(null);
    }

    public void testMatchTailWithMissingHostname() throws Exception {
        final String dataStreamName = "test-logsdb-match-tail-with-missing-hostname";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "text"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:01\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:02\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:03\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:04\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:05\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:06\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:07\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:08\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:09\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:10\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:11\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:12\",\"test_id\": \"%id%\"}"), };

        createDataStream(dataStreamName, mapping, s -> s.put("index.mapping.use_doc_values_skipper", "true"));

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);
        client().admin().indices().prepareRefresh().execute().actionGet();

        Index backingIndex = getBackingIndex(dataStreamName);
        checkTailSkipping(backingIndex, true);
    }

    public void testMatchTailWithSingletonHostname() throws Exception {
        final String dataStreamName = "test-logsdb-match-tail-with-singleton-hostname";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "text"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:01\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:02\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:03\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:04\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:05\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:06\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:07\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:08\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:09\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:10\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:11\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:12\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"), };

        createDataStream(dataStreamName, mapping, s -> s.put("index.mapping.use_doc_values_skipper", "true"));

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);
        client().admin().indices().prepareRefresh().execute().actionGet();

        Index backingIndex = getBackingIndex(dataStreamName);
        checkTailSkipping(backingIndex, true);
    }

    public void testMatchTailWithMultipleHostnames() throws Exception {
        final String dataStreamName = "test-logsdb-match-tail-with-multiple-hostnames";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "test_id": {
                    "type": "text"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:01\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:02\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:03\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:04\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:05\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:06\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:07\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:08\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:09\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:10\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:11\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:12\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"), };

        createDataStream(dataStreamName, mapping, s -> s.put("index.mapping.use_doc_values_skipper", "true"));

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);
        client().admin().indices().prepareRefresh().execute().actionGet();

        Index backingIndex = getBackingIndex(dataStreamName);
        checkTailSkipping(backingIndex, false);
    }

    /**
     * As {@link #testMatchTailWithMultipleHostnames}, with {@code host.name} a {@code multi_value: false} columnar keyword, whose
     * binary doc values hold each document's value as its own bytes. A segment sorted by several hostnames is not sorted by
     * timestamp, whatever framing the hostname was written with.
     */
    public void testMatchTailWithMultipleSingleValuedColumnarHostnames() throws Exception {
        assumeTrue("only meaningful in logsdb_columnar, which this test always uses", "logsdb_columnar".equals(indexMode));
        final String dataStreamName = "test-logsdb-match-tail-with-multiple-single-valued-columnar-hostnames";

        final String mapping = """
            {
              "_doc": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword",
                    "multi_value": false
                  },
                  "test_id": {
                    "type": "text"
                  }
                }
              }
            }
            """;

        final DocWithId[] orderedDocs = {
            doc("{\"@timestamp\":\"2025-01-01T13:00:00\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:01\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:02\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:03\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:04\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:05\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:06\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:07\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:08\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:09\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:10\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:11\",\"host.name\":\"bar\",\"test_id\": \"%id%\"}"),
            doc("{\"@timestamp\":\"2025-01-01T13:00:12\",\"host.name\":\"foo\",\"test_id\": \"%id%\"}"), };

        createDataStream(dataStreamName, mapping, s -> s.put("index.mapping.use_doc_values_skipper", "true"), "logsdb_columnar");

        List<DocWithId> shuffledDocs = shuffledList(Arrays.asList(orderedDocs));
        indexDocuments(dataStreamName, shuffledDocs);
        client().admin().indices().prepareRefresh().execute().actionGet();

        Index backingIndex = getBackingIndex(dataStreamName);
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(backingIndex);
        // Without this the test would prove nothing if host.name did not reach the single-valued framing.
        assertEquals(
            KeywordFieldMapper.KeywordFieldType.DocValuesDiskFormat.BINARY_COLUMNAR_SINGLE_VALUE,
            ((KeywordFieldMapper.KeywordFieldType) indexService.mapperService().fieldType("host.name")).diskFormat()
        );
        checkTailSkipping(backingIndex, false);
    }

    private void createDataStream(String dataStreamName, String mapping) throws IOException {
        createDataStream(dataStreamName, mapping, UnaryOperator.identity());
    }

    private void createDataStream(String dataStreamName, String mapping, UnaryOperator<Settings.Builder> settings) throws IOException {
        createDataStream(dataStreamName, mapping, settings, indexMode);
    }

    private void createDataStream(String dataStreamName, String mapping, UnaryOperator<Settings.Builder> settings, String indexMode)
        throws IOException {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        settings.apply(indexSettings(1, 0)).put("index.mode", indexMode).build(),
                        new CompressedXContent(mapping),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();

        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).actionGet();
    }

    private void indexDocuments(String dataStreamName, List<DocWithId> docs) {
        BulkRequest bulkRequest = new BulkRequest(dataStreamName);
        for (DocWithId doc : docs) {
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(doc.source(), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        var bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
    }

    private Index getBackingIndex(String dataStreamName) {
        var getDataStreamRequest = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        getDataStreamRequest.indicesOptions(
            IndicesOptions.builder()
                .wildcardOptions(
                    IndicesOptions.WildcardOptions.builder()
                        .includeHidden(true)
                        .matchOpen(true)
                        .matchClosed(true)
                        .allowEmptyExpressions(true)
                        .build()
                )
                .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveAliases(false).build())
                .build()
        );

        var response = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        var dataStreams = response.getDataStreams();
        assertThat(dataStreams, hasSize(1));

        var indices = dataStreams.getFirst().getDataStream().getIndices();
        assertThat(indices, hasSize(1));

        return indices.getFirst();
    }

    private void assertSettings(Index backingIndex, Consumer<Settings> settingsTest) {
        final GetSettingsResponse getSettingsResponse = indicesAdmin().getSettings(
            new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex.getName()).includeDefaults(true)
        ).actionGet();
        final Settings combinedSettings = Settings.builder()
            .put(getSettingsResponse.getIndexToDefaultSettings().get(backingIndex.getName()))
            .put(getSettingsResponse.getIndexToSettings().get(backingIndex.getName()))
            .build();
        settingsTest.accept(combinedSettings);
    }

    private void assertOrder(Index backingIndex, DocWithId[] orderedDocs) throws IOException {
        indicesAdmin().forceMerge(new ForceMergeRequest(backingIndex.getName()).maxNumSegments(1).flush(true)).actionGet();

        IndexService indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(backingIndex);
        assertThat(indexService.numberOfShards(), equalTo(1));
        IndexShard shard = indexService.getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            DirectoryReader directoryReader = searcher.getDirectoryReader();

            assertThat(directoryReader.numDocs(), equalTo(orderedDocs.length));

            var segments = directoryReader.leaves();
            assertThat(segments, hasSize(1));

            var segment = segments.getFirst();
            var reader = segment.reader();

            // Read [test_id] through fielddata so the order check is independent of the keyword doc-values format: low-cardinality uses
            // sorted-set doc values while high-cardinality (columnar default) uses binary doc values, and fielddata abstracts over both.
            MappedFieldType testIdField = shard.mapperService().fieldType("test_id");
            SortableBinaryDocValues dvs = testIdField.fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .load(segment)
                .getBytesValues();

            int expectedDocIdx = 0;

            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                String expectedId = orderedDocs[expectedDocIdx++].id;
                assertTrue(dvs.advanceExact(docId));
                assertThat(dvs.docValueCount(), equalTo(1));
                String actualId = dvs.nextValue().utf8ToString();
                assertEquals(expectedId, actualId);
            }
        }
    }

    private void checkTailSkipping(Index backingIndex, boolean expectSkipping) throws IOException {
        indicesAdmin().forceMerge(new ForceMergeRequest(backingIndex.getName()).maxNumSegments(1).flush(true)).actionGet();
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(backingIndex);
        assertThat(indexService.numberOfShards(), equalTo(1));
        IndexShard shard = indexService.getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {

            MappedFieldType timestampField = shard.mapperService().fieldType("@timestamp");
            SortField sf = timestampField.fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .sortField(null, MultiValueMode.MAX, null, false);
            if (sf.getComparatorSource() instanceof LongValuesComparatorSource lv) {
                lv.setMatchTailQuery();
            }

            TopFieldCollectorManager cm = new TopFieldCollectorManager(new Sort(sf), 2, 4);
            LeafCollector leafCollector = cm.newCollector().getLeafCollector(searcher.getLeafContexts().getFirst());
            DocIdSetIterator it = leafCollector.competitiveIterator();

            // collect 5 hits, which should be enough to trigger updating the competitive iterator
            for (int doc = 0; doc < 5; doc++) {
                leafCollector.collect(doc);
            }

            if (expectSkipping) {
                assertThat(it.nextDoc(), greaterThan(5));
            } else {
                assertThat(it.nextDoc(), equalTo(0));
            }

        }
    }
}
