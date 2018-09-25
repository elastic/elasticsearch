/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowIndexAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, transportClientRatio = 0)
public class ShardChangesIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal)  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings(nodeOrdinal));
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Arrays.asList(TestSeedPlugin.class, TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCcr.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    // this emulates what the CCR persistent task will do for pulling
    public void testGetOperationsBasedOnGlobalSequenceId() throws Exception {
        client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .get();

        client().prepareIndex("index", "doc", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();

        ShardStats shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        long globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(2L));

        String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
        ShardChangesAction.Request request =  new ShardChangesAction.Request(shardStats.getShardRouting().shardId(), historyUUID);
        request.setFromSeqNo(0L);
        request.setMaxOperationCount(3);
        ShardChangesAction.Response response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        Translog.Index operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(0L));
        assertThat(operation.id(), equalTo("1"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(1L));
        assertThat(operation.id(), equalTo("2"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(2L));
        assertThat(operation.id(), equalTo("3"));

        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "4").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "5").setSource("{}", XContentType.JSON).get();

        shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(5L));

        request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId(), historyUUID);
        request.setFromSeqNo(3L);
        request.setMaxOperationCount(3);
        response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(3L));
        assertThat(operation.id(), equalTo("3"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(4L));
        assertThat(operation.id(), equalTo("4"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(5L));
        assertThat(operation.id(), equalTo("5"));
    }

    public void testFollowIndex() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureYellow("index1");

        final FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        final int firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> firstBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] firstBatchShardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : firstBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                firstBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, firstBatchNumDocsPerShard));

        for (int i = 0; i < firstBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }

        unfollowIndex("index2");
        client().execute(FollowIndexAction.INSTANCE, followRequest).get();
        final int secondBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> secondBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] secondBatchShardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : secondBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                final long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                secondBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, secondBatchNumDocsPerShard));

        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), numberOfPrimaryShards);
    }

    public void testSyncMappings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureYellow("index1");

        final FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(client().prepareSearch("index2").get().getHits().totalHits, equalTo(firstBatchNumDocs)));
        MappingMetaData mappingMetaData = client().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetaData.sourceAsMap()), nullValue());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"k\":%d}", i);
            client().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(client().prepareSearch("index2").get().getHits().totalHits,
            equalTo(firstBatchNumDocs + secondBatchNumDocs)));
        mappingMetaData = client().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k.type", mappingMetaData.sourceAsMap()), equalTo("long"));
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), 2);
    }

    public void testNoMappingDefined() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        ensureGreen("index1");

        final FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        client().prepareIndex("index1", "doc", "1").setSource("{\"f\":1}", XContentType.JSON).get();
        assertBusy(() -> assertThat(client().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));
        unfollowIndex("index2");

        MappingMetaData mappingMetaData = client().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("long"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetaData.sourceAsMap()), nullValue());
    }

    public void testFollowIndex_backlog() throws Exception {
        int numberOfShards = between(1, 5);
        String leaderIndexSettings = getIndexSettings(numberOfShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
        };
        BulkProcessor bulkProcessor = BulkProcessor.builder(client(), listener)
            .setBulkActions(100)
            .setConcurrentRequests(4)
            .build();
        AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                IndexRequest indexRequest = new IndexRequest("index1", "doc")
                    .source(source, XContentType.JSON)
                    .timeout(TimeValue.timeValueSeconds(1));
                bulkProcessor.add(indexRequest);
            }
        });
        thread.start();

        // Waiting for some document being index before following the index:
        int maxReadSize = randomIntBetween(128, 2048);
        long numDocsIndexed = Math.min(3000 * 2, randomLongBetween(maxReadSize, maxReadSize * 10));
        atLeastDocsIndexed("index1", numDocsIndexed / 3);

        FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        followRequest.setMaxBatchOperationCount(maxReadSize);
        followRequest.setMaxConcurrentReadBatches(randomIntBetween(2, 10));
        followRequest.setMaxConcurrentWriteBatches(randomIntBetween(2, 10));
        followRequest.setMaxWriteBufferSize(randomIntBetween(1024, 10240));
        CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        atLeastDocsIndexed("index1", numDocsIndexed);
        run.set(false);
        thread.join();
        assertThat(bulkProcessor.awaitClose(1L, TimeUnit.MINUTES), is(true));

        assertSameDocCount("index1", "index2");
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), numberOfShards);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33337")
    public void testFollowIndexAndCloseNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        String leaderIndexSettings = getIndexSettings(3, 1, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("index1");

        AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                try {
                    client().prepareIndex("index1", "doc")
                        .setSource(source, XContentType.JSON)
                        .setTimeout(TimeValue.timeValueSeconds(1))
                        .get();
                } catch (Exception e) {
                    logger.error("Error while indexing into leader index", e);
                }
            }
        });
        thread.start();

        FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        followRequest.setMaxBatchOperationCount(randomIntBetween(32, 2048));
        followRequest.setMaxConcurrentReadBatches(randomIntBetween(2, 10));
        followRequest.setMaxConcurrentWriteBatches(randomIntBetween(2, 10));
        client().execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest)).get();

        long maxNumDocsReplicated = Math.min(1000, randomLongBetween(followRequest.getMaxBatchOperationCount(),
            followRequest.getMaxBatchOperationCount() * 10));
        long minNumDocsReplicated = maxNumDocsReplicated / 3L;
        logger.info("waiting for at least [{}] documents to be indexed and then stop a random data node", minNumDocsReplicated);
        atLeastDocsIndexed("index2", minNumDocsReplicated);
        internalCluster().stopRandomNonMasterNode();
        logger.info("waiting for at least [{}] documents to be indexed", maxNumDocsReplicated);
        atLeastDocsIndexed("index2", maxNumDocsReplicated);
        run.set(false);
        thread.join();

        assertSameDocCount("index1", "index2");
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), 3);
    }

    public void testFollowIndexWithNestedField() throws Exception {
        final String leaderIndexSettings =
            getIndexSettingsWithNestedMapping(1, between(0, 1), singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureGreen("index1");

        final FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        client().execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest)).get();

        final int numDocs = randomIntBetween(2, 64);
        for (int i = 0; i < numDocs; i++) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.field("field", "value");
                builder.startArray("objects");
                {
                    builder.startObject();
                    builder.field("field", i);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(builder).get();
            }
        }

        for (int i = 0; i < numDocs; i++) {
            int value = i;
            assertBusy(() -> {
                final GetResponse getResponse = client().prepareGet("index2", "doc", Integer.toString(value)).get();
                assertTrue(getResponse.isExists());
                assertTrue((getResponse.getSource().containsKey("field")));
                assertThat(XContentMapValues.extractValue("objects.field", getResponse.getSource()),
                    equalTo(Collections.singletonList(value)));
            });
        }
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), 1);
    }

    public void testUnfollowNonExistingIndex() {
        UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
        unfollowRequest.setFollowIndex("non-existing-index");
        expectThrows(IllegalArgumentException.class, () -> client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).actionGet());
    }

    public void testFollowNonExistentIndex() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test-leader").get());
        assertAcked(client().admin().indices().prepareCreate("test-follower").get());
        // Leader index does not exist.
        FollowIndexAction.Request followRequest1 = createFollowRequest("non-existent-leader", "test-follower");
        expectThrows(IndexNotFoundException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest1).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest1))
                .actionGet());
        // Follower index does not exist.
        FollowIndexAction.Request followRequest2 = createFollowRequest("non-test-leader", "non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest2).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest2))
                .actionGet());
        // Both indices do not exist.
        FollowIndexAction.Request followRequest3 = createFollowRequest("non-existent-leader", "non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest3).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest3))
                .actionGet());
    }

    public void testFollowIndexMaxOperationSizeInBytes() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureYellow("index1");

        final int numDocs = 1024;
        logger.info("Indexing [{}] docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        followRequest.setMaxOperationSizeInBytes(1L);
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        final Map<ShardId, Long> firstBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] firstBatchShardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : firstBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                firstBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(1, firstBatchNumDocsPerShard));
        for (int i = 0; i < numDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveIndex("index1"), resolveIndex("index2"), 1);
    }

    public void testDontFollowTheWrongIndex() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0,
            Collections.singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("index1");
        assertAcked(client().admin().indices().prepareCreate("index3").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("index3");

        FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        followRequest = createFollowRequest("index3", "index4");
        createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();
        unfollowIndex("index2", "index4");

        FollowIndexAction.Request wrongRequest1 = createFollowRequest("index1", "index4");
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> client().execute(FollowIndexAction.INSTANCE, wrongRequest1).actionGet());
        assertThat(e.getMessage(), containsString("follow index [index4] should reference"));

        FollowIndexAction.Request wrongRequest2 = createFollowRequest("index3", "index2");
        e = expectThrows(IllegalArgumentException.class, () -> client().execute(FollowIndexAction.INSTANCE, wrongRequest2).actionGet());
        assertThat(e.getMessage(), containsString("follow index [index2] should reference"));
    }

    public void testAttemptToChangeCcrFollowingIndexSetting() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureYellow("index1");
        FollowIndexAction.Request followRequest = createFollowRequest("index1", "index2");
        CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();
        unfollowIndex("index2");
        client().admin().indices().close(new CloseIndexRequest("index2")).actionGet();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("index2");
        updateSettingsRequest.settings(Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), false).build());
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> client().admin().indices().updateSettings(updateSettingsRequest).actionGet());
        assertThat(e.getMessage(), equalTo("can not update internal setting [index.xpack.ccr.following_index]; " +
            "this setting is managed via a dedicated API"));
    }

    private CheckedRunnable<Exception> assertTask(final int numberOfPrimaryShards, final Map<ShardId, Long> numDocsPerShard) {
        return () -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData taskMetadata = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            listTasksRequest.setActions(ShardFollowTask.NAME + "[c]");
            ListTasksResponse listTasksResponse = client().admin().cluster().listTasks(listTasksRequest).actionGet();
            assertThat(listTasksResponse.getNodeFailures().size(), equalTo(0));
            assertThat(listTasksResponse.getTaskFailures().size(), equalTo(0));

            List<TaskInfo> taskInfos = listTasksResponse.getTasks();
            assertThat(taskInfos.size(), equalTo(numberOfPrimaryShards));
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> shardFollowTasks =
                taskMetadata.findTasks(ShardFollowTask.NAME, Objects::nonNull);
            for (PersistentTasksCustomMetaData.PersistentTask<?> shardFollowTask : shardFollowTasks) {
                final ShardFollowTask shardFollowTaskParams = (ShardFollowTask) shardFollowTask.getParams();
                TaskInfo taskInfo = null;
                String expectedId = "id=" + shardFollowTask.getId();
                for (TaskInfo info : taskInfos) {
                    if (expectedId.equals(info.getDescription())) {
                        taskInfo = info;
                        break;
                    }
                }
                assertThat(taskInfo, notNullValue());
                ShardFollowNodeTaskStatus status = (ShardFollowNodeTaskStatus) taskInfo.getStatus();
                assertThat(status, notNullValue());
                assertThat("incorrect global checkpoint " + shardFollowTaskParams,
                    status.followerGlobalCheckpoint(),
                    equalTo(numDocsPerShard.get(shardFollowTaskParams.getLeaderShardId())));
            }
        };
    }

    private void unfollowIndex(String... indices) throws Exception {
        for (String index : indices) {
            final UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
            unfollowRequest.setFollowIndex(index);
            client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).get();
        }
        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks().size(), equalTo(0));

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            ListTasksResponse listTasksResponse = client().admin().cluster().listTasks(listTasksRequest).get();
            int numNodeTasks = 0;
            for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                if (taskInfo.getAction().startsWith(ListTasksAction.NAME) == false) {
                    numNodeTasks++;
                }
            }
            assertThat(numNodeTasks, equalTo(0));
        }, 30, TimeUnit.SECONDS);
    }

    private CheckedRunnable<Exception> assertExpectedDocumentRunnable(final int value) {
        return () -> {
            final GetResponse getResponse = client().prepareGet("index2", "doc", Integer.toString(value)).get();
            assertTrue("Doc with id [" + value + "] is missing", getResponse.isExists());
            assertTrue((getResponse.getSource().containsKey("f")));
            assertThat(getResponse.getSource().get("f"), equalTo(value));
        };
    }

    private String getIndexSettings(final int numberOfShards, final int numberOfReplicas,
                                    final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfShards);
                    builder.field("index.number_of_replicas", numberOfReplicas);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("f");
                            {
                                builder.field("type", "integer");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    private String getIndexSettingsWithNestedMapping(final int numberOfShards, final int numberOfReplicas,
                                                     final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfShards);
                    builder.field("index.number_of_replicas", numberOfReplicas);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("objects");
                            {
                                builder.field("type", "nested");
                                builder.startObject("properties");
                                {
                                    builder.startObject("field");
                                    {
                                        builder.field("type", "long");
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                            builder.startObject("field");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    private void atLeastDocsIndexed(String index, long numDocsReplicated) throws InterruptedException {
        logger.info("waiting for at least [{}] documents to be indexed into index [{}]", numDocsReplicated, index);
        awaitBusy(() -> {
            refresh(index);
            SearchRequest request = new SearchRequest(index);
            request.source(new SearchSourceBuilder().size(0));
            SearchResponse response = client().search(request).actionGet();
            return response.getHits().getTotalHits() >= numDocsReplicated;
        }, 60, TimeUnit.SECONDS);
    }

    private void assertSameDocCount(String index1, String index2) throws Exception {
        refresh(index1);
        SearchRequest request1 = new SearchRequest(index1);
        request1.source(new SearchSourceBuilder().size(0));
        SearchResponse response1 = client().search(request1).actionGet();
        assertBusy(() -> {
            refresh(index2);
            SearchRequest request2 = new SearchRequest(index2);
            request2.source(new SearchSourceBuilder().size(0));
            SearchResponse response2 = client().search(request2).actionGet();
            assertThat(response2.getHits().getTotalHits(), equalTo(response1.getHits().getTotalHits()));
        }, 60, TimeUnit.SECONDS);
    }

    private void assertMaxSeqNoOfUpdatesIsTransferred(Index leaderIndex, Index followerIndex, int numberOfShards) throws Exception {
        assertBusy(() -> {
            long[] msuOnLeader = new long[numberOfShards];
            for (int i = 0; i < msuOnLeader.length; i++) {
                msuOnLeader[i] = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            Set<String> leaderNodes = internalCluster().nodesInclude(leaderIndex.getName());
            for (String leaderNode : leaderNodes) {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, leaderNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            msuOnLeader[i] = SequenceNumbers.max(msuOnLeader[i], shard.getMaxSeqNoOfUpdatesOrDeletes());
                        } catch (AlreadyClosedException ignored) {
                            return;
                        }
                    }
                }
            }

            Set<String> followerNodes = internalCluster().nodesInclude(followerIndex.getName());
            for (String followerNode : followerNodes) {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, followerNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(msuOnLeader[i]));
                        } catch (AlreadyClosedException ignored) {

                        }
                    }
                }
            }
        });
    }

    public static FollowIndexAction.Request createFollowRequest(String leaderIndex, String followerIndex) {
        FollowIndexAction.Request request = new FollowIndexAction.Request();
        request.setLeaderIndex(leaderIndex);
        request.setFollowerIndex(followerIndex);
        request.setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.setPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }
}
