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

import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessFileDeletionIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s");
    }

    public void testActiveTranslogFilesArePrunedAfterCommit() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        flush(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
    }

    public void testActiveTranslogFilesNotPrunedOnNotStop() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        var blobContainer = indexObjectStoreService.getTranslogBlobContainer();

        internalCluster().stopNode(indexNode);

        for (TranslogReplicator.BlobTranslogFile translogFile : activeTranslogFiles) {
            assertTrue(blobContainer.blobExists(translogFile.blobName()));
        }
    }

    public void testActiveTranslogFilesArePrunedAfterRelocation() throws Exception {
        startMasterOnlyNode();

        String indexNodeA = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeA);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodeB), indexName);

        ensureGreen(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
    }

    public void testActiveTranslogFilesArePrunedCaseWithMultipleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexNameA = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String indexNameB = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexNameA,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        createIndex(
            indexNameB,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexNameA, indexNameB);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexNameA, randomIntBetween(1, 100));
            indexDocs(indexNameB, randomIntBetween(1, 100));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);
        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        flush(indexNameA);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), greaterThan(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));
        });

        flush(indexNameB);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));

            assertTranslogBlobsDoNotExist(activeTranslogFiles, objectStoreService);
        });
    }

    public void testStaleNodeDoesNotDeleteFile() throws Exception {
        String masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNodeA = startIndexNode(
            Settings.builder()
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        SeqNoStats beforeSeqNoStats = client(indexNodeA).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();

        String indexNodeB = startIndexNode();

        ensureStableCluster(3);

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        MockTransportService indexNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            indexNodeA
        );
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            internalCluster().getMasterName()
        );
        ObjectStoreService indexNodeAObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeA);
        ObjectStoreService indexNodeBObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeB);
        MockRepository repository = (MockRepository) indexNodeBObjectStoreService.getObjectStore();
        repository.setBlockOnAnyFiles();

        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();

        final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false
                && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                removedNode.onResponse(null);
            }
        });

        try {
            masterTransportService.addUnresponsiveRule(indexNodeTransportService);
            removedNode.actionGet();

            // Slight delay to allow the new node to start recovering from an old commit before the new commit is triggered
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

            client(indexNodeA).admin().indices().prepareFlush(indexName).execute().actionGet();

            assertBusy(() -> {
                assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
                assertThat(translogReplicator.getTranslogFilesToDelete().size(), greaterThan(0));
            });
            assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        } finally {
            masterTransportService.clearAllRules();
        }

        assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
        assertThat(translogReplicator.getTranslogFilesToDelete().size(), greaterThan(0));
        assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        repository.unblock();

        assertBusy(() -> {
            assertThat(translogReplicator.getTranslogFilesToDelete().size(), equalTo(0));
            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexNodeAObjectStoreService);
        });

        ensureGreen(indexName);

        SeqNoStats afterSeqNoStats = client(indexNodeB).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        assertEquals(beforeSeqNoStats.getMaxSeqNo(), afterSeqNoStats.getMaxSeqNo());
    }

    private static void assertTranslogBlobsExist(
        Set<TranslogReplicator.BlobTranslogFile> shouldExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(objectStoreService.getTranslogBlobContainer().blobExists(translogFile.blobName()));
        }
    }

    private static void assertTranslogBlobsDoNotExist(
        Set<TranslogReplicator.BlobTranslogFile> doNotExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : doNotExist) {
            assertFalse(objectStoreService.getTranslogBlobContainer().blobExists(translogFile.blobName()));
        }
    }
}
