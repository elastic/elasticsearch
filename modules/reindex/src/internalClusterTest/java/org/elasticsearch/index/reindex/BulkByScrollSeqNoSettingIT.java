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
import org.elasticsearch.action.ActionRequestValidationException;
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
import java.util.function.LongPredicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BulkByScrollSeqNoSettingIT extends ReindexTestCase {

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
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        boolean disableSequenceNumbers = randomBoolean();
        createIndex("test-index", disableSeqNoSettings(disableSequenceNumbers));
        indexDoc("test-index", "1", "field", "value");
        refresh("test-index");

        CountDownLatch searchLatch = assertSearchSeqNoFlag(disableSequenceNumbers == false);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            "test-index",
            seqNo -> disableSequenceNumbers ? seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : seqNo >= 0
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
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        boolean disableSequenceNumbers = randomBoolean();
        createIndex("test-index", disableSeqNoSettings(disableSequenceNumbers));
        indexDoc("test-index", "1", "field", "value");
        refresh("test-index");

        CountDownLatch searchLatch = assertSearchSeqNoFlag(disableSequenceNumbers == false);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            "test-index",
            seqNo -> disableSequenceNumbers ? seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : seqNo >= 0
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

    public void testPatternMatchingMultipleIndicesWithMixedSettingsRejects() {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        createIndex("test-index-1", disableSeqNoSettings(true));
        createIndex("test-index-2", indexSettings(1, 0).build());
        indexDoc("test-index-1", "1", "field", "value");
        indexDoc("test-index-2", "1", "field", "value");
        refresh("test-index-*");

        var updateByQuery = updateByQuery().source("test-index-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, updateByQuery::get);
        assertThat(e.getMessage(), containsString(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey()));
    }

    public void testPatternMatchingMultipleIndicesWithSameSeqNoDisabledSetting() {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        boolean disableSequenceNumbers = randomBoolean();
        Settings seqNoSettings = disableSeqNoSettings(disableSequenceNumbers);
        createIndex("test-index-1", seqNoSettings);
        createIndex("test-index-2", seqNoSettings);
        indexDoc("test-index-1", "1", "field", "value");
        indexDoc("test-index-2", "1", "field", "value");
        refresh("test-index-*");

        CountDownLatch searchLatch = assertSearchSeqNoFlag(disableSequenceNumbers == false);
        LongPredicate seqNoPredicate = seqNo -> disableSequenceNumbers ? seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : seqNo >= 0;
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(Map.of("test-index-1", seqNoPredicate, "test-index-2", seqNoPredicate));
        var updateByQuery = updateByQuery().source("test-index-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testDataStreamWithSeqNoDisabledOnAllBackingIndices() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "my-data-stream";
        createDataStreamWithTemplate(dsName, disableSeqNoTemplateSettings(true));

        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);
        refresh(dsName);
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            dataStream.getWriteIndex().getName(),
            seqNo -> seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
        );
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(numDocs));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    /**
     * When backing indices have mixed settings, the resolver disables seq_no because search hits
     * from backing indices with seq_no disabled will not carry valid sequence numbers.
     * Because at least one backing index has seq_no disabled, writes with explicit IDs to all
     * backing indices are allowed without OCC.
     */
    public void testDataStreamWithMixedBackingIndices() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "my-data-stream";
        createDataStreamWithTemplate(dsName, Settings.EMPTY);
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, disableSeqNoTemplateSettings(true));
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);
        refresh(dsName);
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        Map<String, LongPredicate> indexSeqNoPredicates = new HashMap<>();
        for (Index index : dataStream.getIndices()) {
            indexSeqNoPredicates.put(index.getName(), seqNo -> seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO);
        }
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(indexSeqNoPredicates);
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(numDocs + numDocs2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    /**
     * A mixed data stream (resolves to disabled) and a regular index with seq_no also disabled share
     * the same resolved setting, so the operation is accepted. The regular index doc is updated without
     * OCC. Because at least one backing index has seq_no disabled, writes with explicit IDs to all
     * backing indices are allowed without OCC.
     */
    public void testMixedDataStreamAndRegularIndexWithSameResolvedSetting() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "test-ds";
        createDataStreamWithTemplate(dsName, Settings.EMPTY);
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, disableSeqNoTemplateSettings(true));
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);

        createIndex("test-regular", disableSeqNoSettings(true));
        indexDoc("test-regular", "1", "field", "value");
        refresh("test-*");
        var dataStream = getDataStream(dsName);

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        LongPredicate seqNoPredicate = seqNo -> seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO;
        Map<String, LongPredicate> indexSeqNoPredicates = new HashMap<>();
        for (Index index : dataStream.getIndices()) {
            indexSeqNoPredicates.put(index.getName(), seqNoPredicate);
        }
        indexSeqNoPredicates.put("test-regular", seqNoPredicate);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(indexSeqNoPredicates);
        var updateByQuery = updateByQuery().source("test-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(1 + numDocs + numDocs2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    /**
     * A mixed data stream (resolves to disabled) and a regular index with seq_no enabled have different
     * resolved settings, so the operation is rejected with a validation error.
     */
    public void testMixedDataStreamAndRegularIndexWithDifferentResolvedSettingRejects() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "test-ds";
        createDataStreamWithTemplate(dsName, Settings.EMPTY);
        indexDocs(dsName, between(1, 5));

        updateDataStreamTemplate(dsName, disableSeqNoTemplateSettings(true));
        rolloverDataStream(dsName);
        indexDocs(dsName, between(1, 5));

        createIndex("test-regular", indexSettings(1, 0).build());
        indexDoc("test-regular", "1", "field", "value");
        refresh("test-*");

        var updateByQuery = updateByQuery().source("test-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, updateByQuery::get);
        assertThat(e.getMessage(), containsString(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey()));
    }

    /**
     * Installs an action filter that intercepts the coordinator-level search request and asserts
     * that the {@code seqNoAndPrimaryTerm} flag on the search source matches the expected value.
     * Returns a latch that counts down once when the search request is observed.
     */
    private CountDownLatch assertSearchSeqNoFlag(boolean expectSeqNoAndPrimaryTerm) {
        CountDownLatch latch = new CountDownLatch(1);
        SearchInterceptorPlugin.searchRequestConsumer.set(searchRequest -> {
            if (searchRequest.source() != null) {
                assertEquals(
                    "seqNoAndPrimaryTerm flag on search request",
                    expectSeqNoAndPrimaryTerm,
                    searchRequest.source().seqNoAndPrimaryTerm()
                );
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
            var nodeSeqNoPredicatesByIndes = nodeEntry.getValue();
            MockTransportService.getInstance(nodeEntry.getKey())
                .addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME + "[p]", (handler, request, channel, task) -> {
                    if (request instanceof TransportReplicationAction.ConcreteShardRequest<?> concreteShardRequest
                        && concreteShardRequest.getRequest() instanceof BulkShardRequest bulkShardRequest) {
                        LongPredicate seqNoPredicate = nodeSeqNoPredicatesByIndes.get(bulkShardRequest.index());
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
