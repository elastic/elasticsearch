/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexTestCase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests verifying that bulk-by-scroll operations (update-by-query, delete-by-query)
 * correctly handle the per-index {@link IndexSettings#DISABLE_SEQUENCE_NUMBERS} setting. When an
 * index has sequence numbers disabled, bulk requests must carry {@link SequenceNumbers#UNASSIGNED_SEQ_NO};
 * when enabled, they must carry real ({@code >= 0}) sequence numbers. The search phase always requests
 * {@code seqNoAndPrimaryTerm} regardless of the index setting, because the framework needs it for
 * optimistic concurrency control (indices with sequence numbers disabled return sentinel values).
 */
public class BulkByScrollSeqNoSettingIT extends ReindexTestCase {

    public static final LongPredicate SEQ_NO_DISABLED_MATCHER = seqNo -> seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO;
    public static final LongPredicate SEQ_NO_ENABLED_MATCHER = seqNo -> seqNo >= 0;

    @After
    public void cleanupInterceptors() {
        SearchInterceptorPlugin.searchRequestConsumer.set(null);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node).clearAllRules();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            DataStreamsPlugin.class,
            MockTransportService.TestPlugin.class,
            SearchInterceptorPlugin.class
        );
    }

    public static class SearchInterceptorPlugin extends Plugin implements ActionPlugin {
        private static final AtomicReference<Consumer<SearchRequest>> searchRequestConsumer = new AtomicReference<>();

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new MappedActionFilter() {
                @Override
                public String actionName() {
                    return TransportSearchAction.NAME;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    var consumer = searchRequestConsumer.get();
                    if (consumer != null && request instanceof SearchRequest searchRequest) {
                        consumer.accept(searchRequest);
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    public void testUpdateByQueryOnRegularIndex() {
        boolean disableSequenceNumbers = randomBoolean();
        createIndex("test-index", disableSeqNoSettings(disableSequenceNumbers));
        indexDoc("test-index", "1", "field", "value");
        refresh("test-index");

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            "test-index",
            disableSequenceNumbers ? SEQ_NO_DISABLED_MATCHER : SEQ_NO_ENABLED_MATCHER
        );
        var updateByQuery = updateByQuery().source("test-index");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(1));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testDeleteByQueryOnRegularIndex() {
        boolean disableSequenceNumbers = randomBoolean();
        createIndex("test-index", disableSeqNoSettings(disableSequenceNumbers));
        indexDoc("test-index", "1", "field", "value");
        refresh("test-index");

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            "test-index",
            disableSequenceNumbers ? SEQ_NO_DISABLED_MATCHER : SEQ_NO_ENABLED_MATCHER
        );
        var deleteByQuery = deleteByQuery().source("test-index").filter(QueryBuilders.matchAllQuery());
        if (randomBoolean()) {
            deleteByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = deleteByQuery.get();
        assertThat(response, matcher().deleted(1));
        refresh("test-index");
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testPatternMatchingMultipleIndicesWithMixedSettings() {
        createIndex("test-index-1", disableSeqNoSettings(randomBoolean()));
        createIndex("test-index-2", disableSeqNoSettings(randomBoolean()));
        indexDoc("test-index-1", "1", "field", "value");
        indexDoc("test-index-2", "1", "field", "value");
        refresh("test-index-*");

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            Stream.of("test-index-1", "test-index-2")
                .collect(Collectors.toMap(Function.identity(), BulkByScrollSeqNoSettingIT::getBulkSeqNoMatcherForIndex))
        );
        var updateByQuery = updateByQuery().source("test-index-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(2));
        safeAwait(bulkLatch);
        safeAwait(searchLatch);
    }

    public void testDataStreamWithSeqNoDisabledOnAllBackingIndices() throws Exception {
        String dsName = "my-data-stream";
        createDataStreamWithTemplate(dsName, disableSeqNoTemplateSettings(true));

        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);
        refresh(dsName);
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(dataStream.getWriteIndex().getName(), SEQ_NO_DISABLED_MATCHER);
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(numDocs));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testDataStreamWithMixedBackingIndices() throws Exception {
        String dsName = "my-data-stream";
        createDataStreamWithTemplate(dsName, disableSeqNoSettings(randomBoolean()));
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, disableSeqNoTemplateSettings(randomBoolean()));
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);
        refresh(dsName);
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            dataStream.getIndices()
                .stream()
                .collect(Collectors.toMap(Index::getName, index -> getBulkSeqNoMatcherForIndex(index.getName())))
        );
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(numDocs + numDocs2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testMixedDataStreamAndRegularIndex() throws Exception {
        String dsName = "test-ds";
        createDataStreamWithTemplate(dsName, disableSeqNoSettings(randomBoolean()));
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, disableSeqNoTemplateSettings(randomBoolean()));
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);

        createIndex("test-regular", disableSeqNoSettings(randomBoolean()));
        indexDoc("test-regular", "1", "field", "value");
        refresh("test-*");
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchAlwaysRequestsSeqNo();
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            Stream.concat(Stream.of("test-regular"), dataStream.getIndices().stream().map(Index::getName))
                .collect(Collectors.toMap(Function.identity(), BulkByScrollSeqNoSettingIT::getBulkSeqNoMatcherForIndex))
        );
        var updateByQuery = updateByQuery().source("test-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(1 + numDocs + numDocs2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    private static LongPredicate getBulkSeqNoMatcherForIndex(String index) {
        var seqNoDisabled = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, index)
            .get()
            .getIndexToSettings()
            .get(index)
            .getAsBoolean(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), false);
        return seqNoDisabled ? SEQ_NO_DISABLED_MATCHER : SEQ_NO_ENABLED_MATCHER;
    }

    /**
     * Installs an action filter that intercepts the coordinator-level search request and asserts
     * that the {@code seqNoAndPrimaryTerm} flag on the search source is set to true.
     * Returns a latch that counts down once when the search request is observed.
     */
    private CountDownLatch assertSearchAlwaysRequestsSeqNo() {
        CountDownLatch latch = new CountDownLatch(1);
        SearchInterceptorPlugin.searchRequestConsumer.set(searchRequest -> {
            if (searchRequest.source() != null) {
                assertEquals("seqNoAndPrimaryTerm flag on search request", true, searchRequest.source().seqNoAndPrimaryTerm());
                latch.countDown();
            }
        });
        return latch;
    }

    /**
     * Intercepts shard-level bulk requests on the node holding the primary for the given index and
     * asserts that the ifSeqNo of every item matches the given predicate. Returns a latch that counts
     * down when at least one matching bulk request has been observed.
     */
    private CountDownLatch assertBulkShardRequestsMatch(String indexName, LongPredicate seqNoPredicate) {
        return assertBulkShardRequestsMatch(Map.of(indexName, seqNoPredicate));
    }

    /**
     * Intercepts shard-level bulk requests for multiple indices and asserts that the ifSeqNo of every
     * item matches the predicate associated with that index. Installs a single handler per node to
     * avoid overriding previous handlers when multiple indices share a primary node. Returns a latch
     * that counts down once per index when a matching bulk request has been observed.
     */
    private CountDownLatch assertBulkShardRequestsMatch(Map<String, LongPredicate> indexSeqNoPredicates) {
        CountDownLatch latch = new CountDownLatch(indexSeqNoPredicates.size());
        Map<String, Map<String, LongPredicate>> byNode = new HashMap<>();
        for (var entry : indexSeqNoPredicates.entrySet()) {
            String nodeName = primaryNodeName(entry.getKey());
            byNode.computeIfAbsent(nodeName, k -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }
        for (var nodeEntry : byNode.entrySet()) {
            var nodeSeqNoPredicatesByIndex = nodeEntry.getValue();
            MockTransportService.getInstance(nodeEntry.getKey())
                .addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME + "[p]", (handler, request, channel, task) -> {
                    if (request instanceof TransportReplicationAction.ConcreteShardRequest<?> concreteShardRequest
                        && concreteShardRequest.getRequest() instanceof BulkShardRequest bulkShardRequest) {
                        LongPredicate seqNoPredicate = nodeSeqNoPredicatesByIndex.get(bulkShardRequest.index());
                        assertThat(seqNoPredicate, is(notNullValue()));
                        boolean allMatch = Arrays.stream(bulkShardRequest.items())
                            .allMatch(item -> seqNoPredicate.test(item.request().ifSeqNo()));
                        assertTrue("ifSeqNo on bulk item did not match expected predicate", allMatch);
                        latch.countDown();
                    }
                    handler.messageReceived(request, channel, task);
                });
        }
        return latch;
    }

    private String primaryNodeName(String indexName) {
        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        String nodeId = state.routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        return state.nodes().get(nodeId).getName();
    }

    private void createDataStreamWithTemplate(String dsName, Settings settings) throws Exception {
        putDataStreamTemplate(dsName, settings);
        CreateDataStreamAction.Request createRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dsName
        );
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest));
    }

    private void putDataStreamTemplate(String dsName, Settings settings) throws Exception {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            dsName + "-template"
        );
        String mapping = """
            {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    }
                }
            }
            """;
        Settings templateSettings = indexSettings(1, 0).put(settings).build();
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dsName + "*"))
                .template(new Template(templateSettings, CompressedXContent.fromJSON(mapping), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    private void updateDataStreamTemplate(String dsName, Settings settings) throws Exception {
        putDataStreamTemplate(dsName, settings);
    }

    private void indexDocs(String dsName, int numDocs) {
        var bulk = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulk.add(prepareIndex(dsName).setCreate(true).setSource("@timestamp", System.currentTimeMillis(), "field", "val-" + i));
        }
        bulk.get();
    }

    private void rolloverDataStream(String dsName) {
        AcknowledgedResponse response = client().admin().indices().prepareRolloverIndex(dsName).get();
        assertTrue(response.isAcknowledged());
    }

    private Settings disableSeqNoSettings(boolean disableSequenceNumbers) {
        Settings.Builder builder = indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), disableSequenceNumbers);
        if (disableSequenceNumbers) {
            builder.put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY);
        }
        return builder.build();
    }

    private static Settings disableSeqNoTemplateSettings(boolean disableSequenceNumbers) {
        Settings.Builder builder = Settings.builder().put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), disableSequenceNumbers);
        if (disableSequenceNumbers) {
            builder.put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY);
        }
        return builder.build();
    }

    private DataStream getDataStream(String dataStreamName) throws ExecutionException, InterruptedException {
        return client().admin()
            .cluster()
            .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
            .get()
            .getState()
            .getMetadata()
            .getProject(ProjectId.DEFAULT)
            .dataStreams()
            .get(dataStreamName);
    }
}
