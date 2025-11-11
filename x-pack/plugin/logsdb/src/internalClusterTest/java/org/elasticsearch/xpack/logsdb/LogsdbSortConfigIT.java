/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.index.DirectoryReader;
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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LogsdbSortConfigIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InternalSettingsPlugin.class, XPackPlugin.class, LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("cluster.logsdb.enabled", "true")
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
                  "@timestmap": {
                    "type": "date"
                  },
                  "host.name": {
                    "type": "keyword"
                  },
                  "message": {
                    "type": "pattern_text"
                  },
                  "test_id": {
                    "type": "text",
                    "store": true
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
                    "type": "text",
                    "store": true
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
                    "type": "text",
                    "store": true
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
                assertThat(IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings), equalTo(List.of("@timestamp")));
                assertThat(IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings), equalTo(List.of(SortOrder.DESC)));
                assertThat(IndexSortConfig.INDEX_SORT_MODE_SETTING.get(settings), equalTo(List.of(MultiValueMode.MAX)));
                assertThat(IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(settings), equalTo(List.of("_last")));
            });
        }

        assertOrder(backingIndex, orderedDocs);
    }

    private void createDataStream(String dataStreamName, String mapping) throws IOException {
        createDataStream(dataStreamName, mapping, UnaryOperator.identity());
    }

    private void createDataStream(String dataStreamName, String mapping, UnaryOperator<Settings.Builder> settings) throws IOException {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        settings.apply(indexSettings(1, 0)).put("index.mode", "logsdb").build(),
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
                        .resolveAliases(false)
                        .build()
                )
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
            var storedFields = reader.storedFields();

            int expectedDocIdx = 0;

            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                String expectedId = orderedDocs[expectedDocIdx++].id;
                String actualId = storedFields.document(docId).get("test_id");
                assertEquals(expectedId, actualId);
            }
        }
    }

}
