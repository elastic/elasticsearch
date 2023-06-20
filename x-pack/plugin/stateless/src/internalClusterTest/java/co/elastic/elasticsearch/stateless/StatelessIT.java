/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicatorReader;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.IndexSettings.STATELESS_DEFAULT_REFRESH_INTERVAL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessIT extends AbstractStatelessIntegTestCase {

    public void testCompoundCommitHasNodeEphemeralId() throws Exception {
        startMasterOnlyNode();

        String indexNodeName = startIndexNodes(1).get(0);
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);

        indexDocuments(indexName);
        // Force a flush if necessary to make sure we have at least one compound commit
        flush(indexName);

        assertObjectStoreConsistentWithIndexShards();

        Index index = resolveIndex(indexName);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeName);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, indexNodeName);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        String commitFile = StatelessCompoundCommit.NAME + Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );
        assertThat(
            "Expected that the compound commit has the ephemeral Id of the indexing node",
            commit.nodeEphemeralId(),
            equalTo(clusterService.localNode().getEphemeralId())
        );
    }

    public void testClusterCanFormWithStatelessEnabled() {
        startMasterOnlyNode();

        final int numIndexNodes = randomIntBetween(1, 5);
        startIndexNodes(numIndexNodes);
        ensureStableCluster(numIndexNodes + 1);

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(Stateless.class).stream())
            .toList();
        assertThat(plugins.size(), greaterThan(0));
    }

    public void testRefreshIntervalSetting() throws Exception {
        startMasterOnlyNode();
        startIndexNodes(1);
        String refreshIntervalSetting = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );

        final String fastIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            fastIndexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );

        final Map<Index, Integer> indices = resolveIndices();
        Index index = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        Index fastIndex = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(fastIndexName)).findAny().get().getKey();

        // Non fast refresh index

        assertNull("setting should not be set", getRefreshIntervalSetting(indexName, false));
        assertEquals(
            "unexpected default value for non fast refresh indices",
            STATELESS_DEFAULT_REFRESH_INTERVAL,
            getRefreshIntervalSetting(indexName, true)
        );
        assertRefreshIntervalConcreteValue(index, STATELESS_DEFAULT_REFRESH_INTERVAL);
        assertTrue(setRefreshIntervalSetting(indexName, TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueSeconds(120), getRefreshIntervalSetting(indexName, false));
        assertRefreshIntervalConcreteValue(index, TimeValue.timeValueSeconds(120));
        // TODO (ES-6244): try setting to less than 5sec and assert there is a validation exception.
        assertTrue(setRefreshIntervalSetting(indexName, TimeValue.MINUS_ONE));
        assertEquals(TimeValue.MINUS_ONE, getRefreshIntervalSetting(indexName, false));
        assertRefreshIntervalConcreteValue(index, TimeValue.MINUS_ONE);
        assertTrue(setRefreshIntervalSetting(indexName, null));
        assertEquals(STATELESS_DEFAULT_REFRESH_INTERVAL, getRefreshIntervalSetting(indexName, true));
        assertRefreshIntervalConcreteValue(index, STATELESS_DEFAULT_REFRESH_INTERVAL);

        // Fast refresh index. The refresh interval setting should behave similarly to stateful.

        assertNull("setting should not be set", getRefreshIntervalSetting(fastIndexName, false));
        assertEquals(
            "unexpected default value for non fast refresh indices",
            TimeValue.timeValueSeconds(1),
            getRefreshIntervalSetting(fastIndexName, true)
        );
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(1));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueSeconds(120), getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(120));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.timeValueMillis(100)));
        assertEquals(TimeValue.timeValueMillis(100), getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueMillis(100));
        assertTrue(setRefreshIntervalSetting(fastIndexName, TimeValue.MINUS_ONE));
        assertEquals(TimeValue.MINUS_ONE, getRefreshIntervalSetting(fastIndexName, false));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.MINUS_ONE);
        assertTrue(setRefreshIntervalSetting(fastIndexName, null));
        assertEquals(TimeValue.timeValueSeconds(1), getRefreshIntervalSetting(fastIndexName, true));
        assertRefreshIntervalConcreteValue(fastIndex, TimeValue.timeValueSeconds(1));
    }

    public void testScheduledRefreshBypassesSearchIdleness() throws Exception {
        startMasterOnlyNode();
        startIndexNodes(1);
        String refreshIntervalSetting = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMillis(1)).build()
        );

        indexDocs(indexName, randomIntBetween(1, 5));

        final Map<Index, Integer> indices = resolveIndices();
        Index index = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        IndexShard indexShard = findIndexShard(index, 0);
        long genBefore = indexShard.commitStats().getGeneration();

        assertBusy(
            () -> assertThat(
                "expected a scheduled refresh to cause the non fast refresh shard to flush and produce a new commit generation",
                indexShard.commitStats().getGeneration(),
                greaterThan(genBefore)
            )
        );
    }

    public void testUploadToObjectStore() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        indexDocuments(indexName);
    }

    public void testTranslogIsSyncedToObjectStoreDuringIndexing() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final Map<Index, Integer> indices = resolveIndices();
        Optional<Map.Entry<Index, Integer>> index = indices.entrySet()
            .stream()
            .filter(e -> indexName.equals(e.getKey().getName()))
            .findFirst();
        assertTrue(index.isPresent());

        Map.Entry<Index, Integer> entry = index.get();
        DiscoveryNode indexNode = findIndexNode(entry.getKey(), 0);
        final ShardId shardId = new ShardId(entry.getKey(), 0);

        indexDocs(indexName, 1);

        // Check that the translog on the object store contains the correct sequence numbers and number of operations
        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode.getName());
        var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), shardId);
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long totalOps = 0;
        Translog.Operation next = reader.next();
        while (next != null) {
            maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
            totalOps++;
            next = reader.next();
        }
        assertThat(maxSeqNo, equalTo(0L));
        assertThat(totalOps, equalTo(1L));
    }

    public void testAllTranslogOperationsAreWrittenToObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        assertReplicatedTranslogConsistentWithShards();
    }

    public void testDownloadNewCommitsFromObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 2);
        startIndexNodes(numberOfShards);
        startSearchNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }
    }

    public void testDownloadNewReplicasFromObjectStore() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        for (int i = 0; i < 3; i++) {
            indexDocuments(indexName);
            flush(indexName);
            assertObjectStoreConsistentWithIndexShards();
        }

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        assertObjectStoreConsistentWithSearchShards();
    }

    public void testCreatesSearchShardsOfClosedIndex() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        indexDocuments(indexName);
        flush(indexName);
        assertObjectStoreConsistentWithIndexShards();

        assertAcked(client().admin().indices().prepareClose(indexName));

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        // TODO assertObjectStoreConsistentWithSearchShards(); doesn't work yet because closing the index incremented the primary term
    }

    public void testSetsRecyclableBigArraysInTranslogReplicator() throws Exception {
        startMasterAndIndexNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertBusy(() -> {
            var bigArrays = internalCluster().getInstance(TranslogReplicator.class).bigArrays();
            assertNotNull(bigArrays);
            assertNotNull(bigArrays.breakerService());
        });
    }

    public void testIndicesSegments() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 3);
        startIndexNodes(numberOfShards);
        final int numberOfReplicas = randomIntBetween(0, 3);
        startSearchNodes(numberOfReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(0, 5);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        if (iters > 0 || randomBoolean()) {
            refresh(indexName);
        }

        var indicesSegments = client().admin().indices().prepareSegments(indexName).get();
        assertThat(indicesSegments.getSuccessfulShards(), equalTo(numberOfShards + numberOfShards * numberOfReplicas));
        assertThat(indicesSegments.getShardFailures().length, equalTo(0));
        assertThat(indicesSegments.getIndices().size(), equalTo(1));

        var indices = indicesSegments.getIndices().get(indexName);
        assertThat(indices, notNullValue());
        assertThat(indices.getShards().size(), equalTo(numberOfShards));

        var index = resolveIndex(indexName);
        for (int shard = 0; shard < numberOfShards; shard++) {
            var shardSegments = indices.getShards().get(shard);
            assertThat(shardSegments, notNullValue());
            assertThat(shardSegments.shards(), notNullValue());
            assertThat(shardSegments.shards().length, equalTo(1 + numberOfReplicas));
            var shardId = new ShardId(index, shard);
            assertThat(shardSegments.shardId(), equalTo(shardId));

            var indexShard = Arrays.stream(shardSegments.shards())
                .filter(segments -> segments.getShardRouting().isPromotableToPrimary())
                .findAny()
                .orElseThrow(() -> new AssertionError("no index shard found for " + shardId));
            assertThat(indexShard, notNullValue());

            var searchShards = Arrays.stream(shardSegments.shards()).filter(segments -> segments.getShardRouting().isSearchable()).toList();
            assertThat(searchShards.size(), equalTo(numberOfReplicas));

            for (var searchShard : searchShards) {
                assertThat(searchShard.getNumberOfSearch(), equalTo(indexShard.getNumberOfSearch()));
                assertThat(searchShard.getNumberOfCommitted(), equalTo(indexShard.getNumberOfCommitted()));
                assertThat(searchShard.getSegments().size(), equalTo(indexShard.getSegments().size()));

                for (var indexShardSegment : indexShard.getSegments()) {
                    var searchShardSegment = searchShard.getSegments()
                        .stream()
                        .filter(segment -> segment.getName().equals(indexShardSegment.getName()))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("search shard has no corresponding segment " + indexShardSegment.getName()));
                    assertThat(searchShardSegment.getGeneration(), equalTo(indexShardSegment.getGeneration()));
                    assertThat(searchShardSegment.isCommitted(), equalTo(indexShardSegment.isCommitted()));
                    assertThat(searchShardSegment.getNumDocs(), equalTo(indexShardSegment.getNumDocs()));
                    assertThat(searchShardSegment.getDeletedDocs(), equalTo(indexShardSegment.getDeletedDocs()));
                    assertThat(searchShardSegment.getSize(), equalTo(indexShardSegment.getSize()));
                    assertThat(searchShardSegment.getVersion(), equalTo(indexShardSegment.getVersion()));
                    assertThat(searchShardSegment.getMergeId(), equalTo(indexShardSegment.getMergeId()));
                    assertThat(searchShardSegment.getAttributes(), equalTo(indexShardSegment.getAttributes()));
                    assertThat(searchShardSegment.getSegmentSort(), equalTo(indexShardSegment.getSegmentSort()));
                }
            }
        }
    }

    public void testUploadToObjectStoreAfterShardIsClosed() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        ensureGreen(indexName);

        if (randomBoolean()) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        assertObjectStoreConsistentWithIndexShards();

        indexDocs(indexName, randomIntBetween(1, 100));

        // block the object store uploading thread pool with tasks before triggering a flush and closing the shard
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        final String uploadThreadPoolName = ThreadPool.Names.SNAPSHOT;
        final int maxUploadTasks = threadPool.info(uploadThreadPoolName).getMax();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch taskStartedLatch = new CountDownLatch(maxUploadTasks);
        for (int i = 0; i < maxUploadTasks; i++) {
            threadPool.executor(uploadThreadPoolName).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    taskStartedLatch.countDown();
                    latch.await();
                }
            });
        }
        try {
            taskStartedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        int active = -1;
        for (var stats : threadPool.stats()) {
            if (stats.name().equals(uploadThreadPoolName)) {
                active = stats.active();
                break;
            }
        }
        assertThat(active, equalTo(maxUploadTasks));

        ActionFuture<FlushResponse> flushFuture = client().admin().indices().prepareFlush(indexName).execute();

        var indexShard = findIndexShard(resolveIndex(indexName), 0);
        // we must hold a ref on the store to allow assertThatObjectStoreIsConsistentWithLastCommit
        indexShard.store().incRef();
        try {
            var future = client().admin().indices().close(new CloseIndexRequest(indexName).waitForActiveShards(ActiveShardCount.NONE));
            latch.countDown();
            assertThatObjectStoreIsConsistentWithLastCommit(indexShard);
            assertAcked(future.actionGet());
            assertEquals(0, flushFuture.actionGet().getFailedShards());
        } finally {
            indexShard.store().decRef();
        }
    }

    protected static TimeValue getRefreshIntervalSetting(String index, boolean includeDefaults) throws Exception {
        var request = new GetSettingsRequest();
        request = request.indices(index).includeDefaults(includeDefaults);
        GetSettingsResponse response = client().admin().indices().getSettings(request).get();
        String value = response.getSetting(index, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
        return TimeValue.parseTimeValue(value, null, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
    };

    protected static void assertRefreshIntervalConcreteValue(Index index, TimeValue refreshInterval) throws Exception {
        boolean found = false;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                found = true;
                assertThat(
                    indexService + " did not match refresh interval",
                    indexService.getIndexSettings().getRefreshInterval(),
                    equalTo(refreshInterval)
                );
            }
        }
        assertThat(found, equalTo(true));
    };

    protected static boolean setRefreshIntervalSetting(String index, TimeValue timeValue) throws Exception {
        var response = client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(index).settings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), timeValue == null ? null : timeValue.getStringRep())
                )
            )
            .get();
        return response.isAcknowledged();
    };
}
