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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

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
        assertThat("" + commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
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

    public void testRefreshIntervalSetting() {
        startMasterOnlyNode();
        startIndexNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String refreshIntervalSetting = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );

        final String indexNameWithExplicit = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexNameWithExplicit,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .put(refreshIntervalSetting, "10s")
                .build()
        );
        ensureGreen(indexName, indexNameWithExplicit);

        GetSettingsResponse response = client().admin().indices().prepareGetSettings(indexName, indexNameWithExplicit).get();
        TimeValue refreshInterval = TimeValue.parseTimeValue(
            response.getSetting(indexName, refreshIntervalSetting),
            refreshIntervalSetting
        );
        assertEquals(TimeValue.timeValueSeconds(5), refreshInterval);

        TimeValue refreshIntervalWithExplicit = TimeValue.parseTimeValue(
            response.getSetting(indexNameWithExplicit, refreshIntervalSetting),
            refreshIntervalSetting
        );
        assertEquals(TimeValue.timeValueSeconds(10), refreshIntervalWithExplicit);
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

}
