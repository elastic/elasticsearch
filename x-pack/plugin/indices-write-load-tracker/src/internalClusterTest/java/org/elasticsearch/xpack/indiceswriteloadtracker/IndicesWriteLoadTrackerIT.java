/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.indiceswriteloadtracker.IndicesWriteLoadStore.INDICES_WRITE_LOAD_DATA_STREAM;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndicesWriteLoadTrackerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            DataStreamsPlugin.class,
            IndexLifecycle.class,
            IndicesWriteLoadTrackerPlugin.class,
            SleepingTokenFilterPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = super.nodeSettings(nodeOrdinal, otherSettings);
        return Settings.builder()
            .put(settings)
            .put(NODE_PROCESSORS_SETTING.getKey(), 1)
            .put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), 1)
            .put(IndicesWriteLoadStatsService.STORE_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(5))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public static class SleepingTokenFilterPlugin extends Plugin implements AnalysisPlugin {

        private final AtomicInteger sleepTime = new AtomicInteger(1000);

        public SleepingTokenFilterPlugin() {}

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("sleeping", (indexSettings, environment, name, settings) -> new TokenFilterFactory() {
                @Override
                public String name() {
                    return "sleeping";
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return new TokenFilter(tokenStream) {
                        @Override
                        public boolean incrementToken() throws IOException {
                            boolean hasMoreTokens = input.incrementToken();

                            if (hasMoreTokens) {
                                try {
                                    Thread.sleep(sleepTime.get());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(e);
                                }
                            }

                            return hasMoreTokens;
                        }
                    };
                }
            });
        }

    }

    public void testILMPolicyIsCreatedAutomatically() throws Exception {
        final var request = new GetLifecycleAction.Request(IndicesWriteLoadTemplateRegistry.ILM_POLICY_NAME);
        assertBusy(() -> {
            try {
                final var response = client().execute(GetLifecycleAction.INSTANCE, request).actionGet();
                assertThat(response.getPolicies(), is(not(empty())));
                boolean policyFound = response.getPolicies()
                    .stream()
                    .map(GetLifecycleAction.LifecyclePolicyResponseItem::getLifecyclePolicy)
                    .anyMatch(lifecyclePolicy -> lifecyclePolicy.getName().equals(IndicesWriteLoadTemplateRegistry.ILM_POLICY_NAME));
                assertThat(Strings.toString(response, true, true), policyFound, is(equalTo(true)));
            } catch (ResourceNotFoundException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testOnlyDataStreamsLoadIsCollected() throws Exception {
        int numberOfRegularIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfRegularIndices; i++) {
            createIndex("test-" + i);
        }

        final var dataStreams = randomList(1, 10, this::randomDataStreamName);

        putComposableIndexTemplateWithSleepingAnalyzer(dataStreams);

        for (String dataStreamName : dataStreams) {
            client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get();
        }
        long timestampAfterDataStreamsCreation = System.currentTimeMillis();

        ensureIndicesWriteLoadIndexIsSearchable();

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributionsForDataStreams(
                timestampAfterDataStreamsCreation,
                dataStreams
            );
            List<String> collectedDataStreams = shardWriteLoadDistributions.stream()
                .map(ShardWriteLoadHistogramSnapshot::dataStream)
                .toList();
            for (String dataStream : dataStreams) {
                assertThat(dataStream, is(in(collectedDataStreams)));
            }
        });
    }

    public void testSamplesAreNotCollectedAfterDeletingADataStream() throws Exception {
        final var dataStreamNames = randomList(2, 10, this::randomDataStreamName);

        putComposableIndexTemplateWithSleepingAnalyzer(dataStreamNames);

        for (String dataStreamName : dataStreamNames) {
            client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get();
        }

        final var timestampAfterDataStreamCreation = System.currentTimeMillis();

        ensureIndicesWriteLoadIndexIsSearchable();

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributions(timestampAfterDataStreamCreation);

            final var storedDataStreamInfo = shardWriteLoadDistributions.stream()
                .map(ShardWriteLoadHistogramSnapshot::dataStream)
                .collect(Collectors.toSet());
            for (String dataStreamName : dataStreamNames) {
                assertThat(dataStreamName, is(in(storedDataStreamInfo)));
            }
        });

        client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(dataStreamNames.get(0))).get();
        final var timestampAfterDataStreamDeletion = System.currentTimeMillis();

        assertBusy(() -> {
            Set<ShardWriteLoadHistogramSnapshot> shardWriteLoadHistogramSnapshots = getShardWriteLoadDistributions(
                timestampAfterDataStreamDeletion
            );
            assertTrue(shardWriteLoadHistogramSnapshots.stream().noneMatch(s -> s.dataStream().equals(dataStreamNames.get(0))));
        });
    }

    public void testSamplesAreNotCollectedAfterRollover() throws Exception {
        final String dataStream = createDataStreamWithSleepingAnalyzer();
        final var timestampAfterDataStreamCreation = System.currentTimeMillis();

        ensureIndicesWriteLoadIndexIsSearchable();

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributionsForDataStreams(
                timestampAfterDataStreamCreation,
                dataStream
            );
            assertThat(shardWriteLoadDistributions, is(not(empty())));
        });

        final var rolloverResponse = client().admin().indices().prepareRolloverIndex(dataStream).get();
        assertThat(rolloverResponse.isRolledOver(), is(equalTo(true)));
        final var timestampAfterDataStreamRollover = new AtomicLong(System.currentTimeMillis());

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributionsForDataStreams(
                timestampAfterDataStreamRollover.get(),
                dataStream
            );
            assertThat(shardWriteLoadDistributions, is(not(empty())));
            // The first stored sample after rollover might contain the last reading from the previous write index,
            // therefore we should search from that point onwards to ensure that we only store samples for the new
            // write index
            timestampAfterDataStreamRollover.set(System.currentTimeMillis());
            for (ShardWriteLoadHistogramSnapshot shardWriteLoadHistogramSnapshot : shardWriteLoadDistributions) {
                assertThat(
                    shardWriteLoadDistributions.stream().map(ShardWriteLoadHistogramSnapshot::shardId).toList().toString(),
                    shardWriteLoadHistogramSnapshot.shardId().getIndex().getName(),
                    is(endsWith("000002"))
                );
            }
        }, 20, TimeUnit.SECONDS);
    }

    private String createDataStreamWithSleepingAnalyzer() throws Exception {
        final var dataStream = randomDataStreamName();
        putComposableIndexTemplateWithSleepingAnalyzer(List.of(dataStream));
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStream)).get();
        return dataStream;
    }

    public void testSamplesAreNotCollectedAfterAShardMovesToADifferentNode() throws Exception {
        final var dataStream = randomDataStreamName();
        putComposableIndexTemplateWithSleepingAnalyzer(List.of(dataStream));
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStream)).get());

        ensureIndicesWriteLoadIndexIsSearchable();

        long timestampBeforeIndexingDocs = System.currentTimeMillis();
        indexDocs(dataStream, 2);

        final var state = client().admin().cluster().prepareState().get().getState();
        final var writeIndex = state.metadata().getIndicesLookup().get(dataStream).getWriteIndex();

        final var dataOnlyNodeName = internalCluster().startDataOnlyNode();

        client().admin()
            .indices()
            .prepareUpdateSettings(writeIndex.getName())
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", dataOnlyNodeName).build())
            .get();
    }

    public void testSamplesAreNotCollectedAfterDisablingService() throws Exception {
        final var dataStream = randomDataStreamName();
        putComposableIndexTemplateWithSleepingAnalyzer(List.of(dataStream));
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStream)).get());

        final var timestampAfterDataStreamIsCreated = System.currentTimeMillis();

        ensureIndicesWriteLoadIndexIsSearchable();

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributionsForDataStreams(
                timestampAfterDataStreamIsCreated,
                dataStream
            );
            final var storedDataStreamInfo = shardWriteLoadDistributions.stream()
                .map(ShardWriteLoadHistogramSnapshot::dataStream)
                .collect(Collectors.toSet());
            assertThat(dataStream, is(in(storedDataStreamInfo)));
        });

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(IndicesWriteLoadStatsService.ENABLED_SETTING.getKey(), false).build())
                .get()
        );

        final var timestampAfterDisablingWriteLoadCollection = System.currentTimeMillis();

        assertBusy(() -> {
            final var shardWriteLoadDistributions = getShardWriteLoadDistributionsForDataStreams(
                timestampAfterDisablingWriteLoadCollection,
                dataStream
            );
            assertThat(shardWriteLoadDistributions, is(emptyIterable()));
        });
    }

    public void testDataStreamWriteLoadIsCollectedAndStored() throws Exception {
        final var dataStream = randomDataStreamName();
        putComposableIndexTemplateWithSleepingAnalyzer(List.of(dataStream));
        final var createDataStreamRequest = new CreateDataStreamAction.Request(dataStream);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        ensureGreen(dataStream);
        long timestampBeforeIndexingDocs = System.currentTimeMillis();
        // Each document takes ~1 second to be indexed since we're using the SleepingTokenFilterPlugin
        // to simulate a CPU intensive indexing op
        indexDocs(dataStream, 20);
        long timestampAfterIndexingDocs = System.currentTimeMillis();

        ensureIndicesWriteLoadIndexIsSearchable();

        assertBusy(() -> {
            final var shardWriteLoadDistributionsForDataStreams = getShardWriteLoadDistributionsForDataStreams(
                timestampBeforeIndexingDocs,
                timestampAfterIndexingDocs,
                List.of(dataStream)
            );
            assertThat(shardWriteLoadDistributionsForDataStreams, is(not(empty())));
            for (ShardWriteLoadHistogramSnapshot shardWriteLoadHistogramSnapshot : shardWriteLoadDistributionsForDataStreams) {
                assertThat(shardWriteLoadHistogramSnapshot.dataStream(), is(equalTo(dataStream)));
                assertThat(
                    Strings.toString(shardWriteLoadHistogramSnapshot, true, true),
                    shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot().max(),
                    is(closeTo(1.0, 0.5))
                );
                assertThat(
                    Strings.toString(shardWriteLoadHistogramSnapshot, true, true),
                    shardWriteLoadHistogramSnapshot.refreshLoadHistogramSnapshot().max(),
                    is(greaterThanOrEqualTo(0.0))
                );
            }
        });
    }

    private void ensureIndicesWriteLoadIndexIsSearchable() throws Exception {
        assertBusy(() -> {
            try {
                client().prepareSearch(INDICES_WRITE_LOAD_DATA_STREAM).get();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    private String randomDataStreamName() {
        return randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
    }

    private void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        String.format(
                            Locale.ROOT,
                            "{\"%s\":\"%s\", \"text1\": \"hola\"}",
                            DEFAULT_TIMESTAMP_FIELD,
                            System.currentTimeMillis()
                        ),
                        XContentType.JSON
                    )
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(
                itemResponse.getFailure() != null ? itemResponse.getFailure().toString() : "",
                itemResponse.getFailureMessage(),
                nullValue()
            );
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private void putComposableIndexTemplateWithSleepingAnalyzer(List<String> patterns) throws Exception {
        var jsonTemplate = """
            {
                "index_patterns": [
                    %s
                ],
                "data_stream": {
                    "hidden": false
                },
                "template": {
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "sleeping_analyzer": {
                                    "tokenizer": "standard",
                                    "filter": [
                                        "sleeping"
                                    ]
                                }
                            }
                        },
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "dynamic": false,
                        "properties": {
                            "@timestamp": {
                                "type": "date",
                                "format": "epoch_millis"
                            },
                            "text1": {
                                "type": "text",
                                "analyzer": "sleeping_analyzer"
                            }
                        }
                    }
                }
            }""".formatted(patterns.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")));
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonTemplate)) {
            final var request = new PutComposableIndexTemplateAction.Request("my-logs");
            request.indexTemplate(ComposableIndexTemplate.parse(parser));
            client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
        }
    }

    private Set<ShardWriteLoadHistogramSnapshot> getShardWriteLoadDistributions(long fromTimestamp) throws IOException {
        return getShardWriteLoadDistributionsForDataStreams(fromTimestamp, Collections.emptyList());
    }

    private Set<ShardWriteLoadHistogramSnapshot> getShardWriteLoadDistributionsForDataStreams(long fromTimestamp, String dataStream)
        throws IOException {
        return getShardWriteLoadDistributionsForDataStreams(fromTimestamp, List.of(dataStream));
    }

    private Set<ShardWriteLoadHistogramSnapshot> getShardWriteLoadDistributionsForDataStreams(long fromTimestamp, List<String> dataStreams)
        throws IOException {
        return getShardWriteLoadDistributionsForDataStreams(fromTimestamp, "now", dataStreams);
    }

    private Set<ShardWriteLoadHistogramSnapshot> getShardWriteLoadDistributionsForDataStreams(
        long fromTimestamp,
        Object toTimestamp,
        List<String> dataStreams
    ) throws IOException {
        final var queryFilter = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("@timestamp").from(fromTimestamp).to(toTimestamp));
        if (dataStreams.size() > 0) {
            queryFilter.filter(QueryBuilders.termsQuery("data_stream", dataStreams));
        }
        final var searchResponse = client().prepareSearch(INDICES_WRITE_LOAD_DATA_STREAM)
            .setQuery(queryFilter)
            .addSort("@timestamp", SortOrder.ASC)
            .setSize(50)
            .get();

        if (searchResponse.getHits().getTotalHits().value == 0) {
            return Collections.emptySet();
        }

        final Set<ShardWriteLoadHistogramSnapshot> shardWriteLoadHistogramSnapshots = new HashSet<>();
        for (final var hit : searchResponse.getHits().getHits()) {
            final var source = hit.getSourceRef();
            assertThat(source, is(notNullValue()));
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, source.streamInput())
            ) {
                shardWriteLoadHistogramSnapshots.add(ShardWriteLoadHistogramSnapshot.fromXContent(parser));
            }
        }
        return shardWriteLoadHistogramSnapshots;
    }

}
