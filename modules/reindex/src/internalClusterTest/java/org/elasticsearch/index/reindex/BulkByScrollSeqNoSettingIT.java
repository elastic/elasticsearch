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
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexSettings;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongPredicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class BulkByScrollSeqNoSettingIT extends ReindexTestCase {

    @After
    public void resetSearchInterceptor() {
        SearchInterceptorPlugin.searchRequestConsumer.set(null);
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
        createIndex("test-index", indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), disableSequenceNumbers).build());
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
        createIndex("test-index", indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), disableSequenceNumbers).build());
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

        createIndex("test-index-1", indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build());
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

    public void testPatternMatchingMultipleIndicesSameSeqNoDisabled() {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        boolean disableSequenceNumbers = randomBoolean();
        Settings seqNoDisabled = indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), disableSequenceNumbers).build();
        createIndex("test-index-1", seqNoDisabled);
        createIndex("test-index-2", seqNoDisabled);
        indexDoc("test-index-1", "1", "field", "value");
        indexDoc("test-index-2", "1", "field", "value");
        refresh("test-index-*");

        CountDownLatch searchLatch = assertSearchSeqNoFlag(disableSequenceNumbers == false);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch(
            "test-index-1",
            seqNo -> disableSequenceNumbers ? seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : seqNo >= 0
        );
        var updateByQuery = updateByQuery().source("test-index-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response, matcher().updated(2));
        safeAwait(searchLatch);
        safeAwait(bulkLatch);
    }

    public void testDataStreamWithSameSettingOnAllBackingIndices() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "my-data-stream";
        Settings dsSettings = Settings.builder().put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build();
        createDataStreamWithTemplate(dsName, dsSettings);

        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);
        refresh(dsName);

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response.getBulkFailures().size(), greaterThan(0));
        assertThat(response.getBulkFailures().get(0).getMessage(), containsString("no if_primary_term and if_seq_no set"));
        safeAwait(searchLatch);
    }

    /**
     * When backing indices have mixed settings, the resolver disables seq_no because search hits
     * from backing indices with seq_no disabled will not carry valid sequence numbers.
     * The write fails at the data stream level, but we verify the failure confirms the intent.
     */
    public void testDataStreamWithMixedBackingIndices() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "my-data-stream";
        createDataStreamWithTemplate(dsName, Settings.EMPTY);
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, Settings.builder().put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build());
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);
        refresh(dsName);

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        var updateByQuery = updateByQuery().source(dsName);
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        // TODO: fix this once overwrites are allowed for backing indices
        assertThat(response.getBulkFailures().size(), greaterThan(0));
        assertThat(response.getBulkFailures().get(0).getMessage(), containsString("no if_primary_term and if_seq_no set"));
        safeAwait(searchLatch);
    }

    /**
     * A mixed data stream (resolves to disabled) and a regular index with seq_no also disabled share
     * the same resolved setting, so the operation is accepted. The regular index doc is updated without
     * OCC, while the data stream docs produce bulk failures due to data stream write protection.
     */
    public void testMixedDataStreamAndRegularIndexWithSameResolvedSetting() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        String dsName = "test-ds";
        createDataStreamWithTemplate(dsName, Settings.EMPTY);
        int numDocs = between(1, 5);
        indexDocs(dsName, numDocs);

        updateDataStreamTemplate(dsName, Settings.builder().put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build());
        rolloverDataStream(dsName);
        int numDocs2 = between(1, 5);
        indexDocs(dsName, numDocs2);

        createIndex("test-regular", indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build());
        indexDoc("test-regular", "1", "field", "value");
        refresh("test-*");

        CountDownLatch searchLatch = assertSearchSeqNoFlag(false);
        CountDownLatch bulkLatch = assertBulkShardRequestsMatch("test-regular", seqNo -> seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO);
        var updateByQuery = updateByQuery().source("test-*");
        if (randomBoolean()) {
            updateByQuery.source().seqNoAndPrimaryTerm(randomBoolean());
        }
        BulkByScrollResponse response = updateByQuery.get();
        assertThat(response.getUpdated(), greaterThan(0L));
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

        updateDataStreamTemplate(dsName, Settings.builder().put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build());
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
        CountDownLatch latch = new CountDownLatch(1);
        String nodeName = primaryNodeName(indexName);
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME + "[p]", (handler, request, channel, task) -> {
                if (request instanceof TransportReplicationAction.ConcreteShardRequest<?> concreteShardRequest
                    && concreteShardRequest.getRequest() instanceof BulkShardRequest bulkShardRequest) {
                    boolean allMatch = Arrays.stream(bulkShardRequest.items())
                        .allMatch(item -> seqNoPredicate.test(item.request().ifSeqNo()));
                    assertTrue("ifSeqNo on bulk item did not match expected predicate", allMatch);
                    latch.countDown();
                }
                handler.messageReceived(request, channel, task);
            });
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
}
