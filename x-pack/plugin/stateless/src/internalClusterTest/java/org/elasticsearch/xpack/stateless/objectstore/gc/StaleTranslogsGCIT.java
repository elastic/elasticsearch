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

package co.elastic.elasticsearch.stateless.objectstore.gc;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class StaleTranslogsGCIT extends AbstractStatelessIntegTestCase {

    public static class TestStateless extends Stateless {

        public ObjectStoreGCTaskExecutor taskExecutor;

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            var l = super.getPersistentTasksExecutor(clusterService, threadPool, client, settingsModule, expressionResolver);
            taskExecutor = (ObjectStoreGCTaskExecutor) l.stream().filter((o) -> o instanceof ObjectStoreGCTaskExecutor).findFirst().get();
            return l;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            super.nodePlugins().stream().map(c -> c.equals(Stateless.class) ? TestStateless.class : c).toList(),
            MockRepository.Plugin.class
        );
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1));
    }

    private static void cleanStaleTranslogs(String node) throws Exception {
        cleanStaleTranslogs(node, 30);
    }

    private static void cleanStaleTranslogs(String node, int secondsTimeout) throws Exception {
        cleanStaleTranslogs(node, secondsTimeout, ActionListener.noop());
    }

    private static void cleanStaleTranslogs(String node, int secondsTimeout, ActionListener<Void> listenerOnConsistentClusterState)
        throws Exception {
        ObjectStoreGCTask gcTask = (ObjectStoreGCTask) internalCluster().getInstance(TransportService.class, node)
            .getTaskManager()
            .getTasks()
            .values()
            .stream()
            .filter(t -> t instanceof ObjectStoreGCTask)
            .findFirst()
            .get();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        gcTask.cleanStaleTranslogs(future, listenerOnConsistentClusterState);
        future.get(secondsTimeout, TimeUnit.SECONDS);
    }

    public void testTranslogGC() throws Exception {
        startMasterOnlyNode();
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();

        String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        String ephemeralIdA = internalCluster().getInstance(ClusterService.class, indexNodeA).localNode().getEphemeralId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(1, 5));

        String indexNodeB = startIndexNode();
        ensureStableCluster(3);
        String ephemeralIdB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();

        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(1, 5));

        if (randomBoolean()) {
            // Test that a RED cluster does not remove stale translog folders
            this.disableAllocation(indexName); // turns the cluster red
            internalCluster().stopNode(indexNodeB); // the stale node
            var translogFilesOfB = objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet();
            ensureStableCluster(1);
            String indexNodeC = startIndexNode(); // will hold the persistent task
            ensureStableCluster(2);
            cleanStaleTranslogs(indexNodeC);
            assertThat(
                "The translog files of node B should have remain intact",
                objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
                is(translogFilesOfB)
            );
            this.enableAllocation(indexName);
            ensureGreen(indexName);
            cleanStaleTranslogs(indexNodeC);
            assertThat(
                "The translog files of node B should have been deleted",
                objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
                is(empty())
            );
        } else {
            assertBusy(() -> {
                assertThat(
                    "The translog files of node A should have been deleted",
                    objectStoreService.getTranslogBlobContainer(ephemeralIdA).listBlobs(OperationPurpose.TRANSLOG).keySet(),
                    is(empty())
                );
            });
        }
    }

    public void testTranslogGCWithDisruption() throws Exception {
        String masterNode = startMasterOnlyNode();
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();

        String indexNodeA = startIndexNode();
        ensureStableCluster(2);

        ClusterService clusterServiceA = internalCluster().getInstance(ClusterService.class, indexNodeA);
        String ephemeralIdA = clusterServiceA.localNode().getEphemeralId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        String indexNodeB = startIndexNode();
        ensureStableCluster(3);
        String ephemeralIdB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();

        logger.info("--> start disrupting cluster");
        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNodeA), Set.of(indexNodeB, masterNode)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(2, masterNode);

        logger.info("--> waiting for index to recover");
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indexName).timeout(TEST_REQUEST_TIMEOUT)
            .waitForStatus(ClusterHealthStatus.GREEN)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true)
            .waitForNodes(Integer.toString(2));
        assertFalse(client(masterNode).admin().cluster().health(healthRequest).actionGet().isTimedOut());

        assertBusy(() -> {
            assertThat(
                "The translog files of node A should have been deleted",
                objectStoreService.getTranslogBlobContainer(ephemeralIdA).listBlobs(OperationPurpose.TRANSLOG).keySet(),
                is(empty())
            );
        });

        logger.info("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();
        ensureStableCluster(3);

        logger.info("--> indexing a few more docs");
        indexDocs(indexName, numDocs);
        assertThat(
            "The translog files of node A should have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdA).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(empty())
        );
        assertThat(
            "Translog files of node B should be present",
            objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(not(empty()))
        );
    }

    public void testIsolatedNodeDoesNotDeleteTranslogs() throws Exception {
        Settings settings = Settings.builder().put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1)).build();
        String masterNode = startMasterOnlyNode(settings);
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
        AtomicInteger nodes = new AtomicInteger(1);

        String indexNodeA = buildNodeWithIndex(nodes, settings); // has the persistent task and will be isolated
        String indexNodeB = buildNodeWithIndex(nodes, settings); // will stop, and its folder should not be deleted by isolated node A
        String indexNodeC = buildNodeWithIndex(nodes, settings); // will have a new persistent task once node A is isolated

        String ephemeralIdB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();
        internalCluster().stopNode(indexNodeB);
        ensureStableCluster(3);

        logger.info("--> start disrupting cluster");
        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNodeA), Set.of(indexNodeC, masterNode)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(2, masterNode);

        expectThrows(
            TimeoutException.class,
            "Should timeout since it cannot find a consistent cluster state",
            () -> cleanStaleTranslogs(indexNodeA, 1)
        );
        assertThat(
            "The translog files of node B should not have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(not(empty()))
        );

        logger.info("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();
        ensureStableCluster(3);

        cleanStaleTranslogs(indexNodeC);
        assertThat(
            "The translog files of node B should not have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(empty())
        );
    }

    public void testNewFilesOfRejoinedNodeAreNotDeleted() throws Exception {
        Settings settings = Settings.builder().put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1)).build();
        String masterNode = startMasterOnlyNode(settings);
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();

        String indexNodeA = startIndexNode(settings); // has the persistent GC task
        ensureStableCluster(2);

        String indexNodeB = startIndexNode(settings); // will be isolated and then will rejoin
        ensureStableCluster(3);
        String ephemeralIdB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();

        // Create a first index on node B, so that some translog files are created
        String initialIndexNameOnNodeB = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            initialIndexNameOnNodeB,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put("index.routing.allocation.require._name", indexNodeB)
                .build()
        );
        ensureGreen(initialIndexNameOnNodeB);
        indexDocs(initialIndexNameOnNodeB, randomIntBetween(1, 10));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", (String) null), initialIndexNameOnNodeB);

        var initialTranslogFiles = objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet();
        logger.info("--> initial translog files {}", initialTranslogFiles);

        logger.info("--> start disrupting [{}]", indexNodeB);
        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNodeA, masterNode), Set.of(indexNodeB)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(2, masterNode);
        logger.info("--> waiting for index to recover");
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, initialIndexNameOnNodeB).timeout(
            TEST_REQUEST_TIMEOUT
        )
            .waitForStatus(ClusterHealthStatus.GREEN)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true)
            .waitForNodes(Integer.toString(2));
        client(masterNode).admin().cluster().health(healthRequest).actionGet();

        // initiate GC, but before deleting translog files, rejoin index node b and create a new index with new translog files
        cleanStaleTranslogs(indexNodeA, 30, ActionListener.wrap((r) -> {
            logger.info("--> stop disrupting cluster");
            networkDisruption.stopDisrupting();
            ensureStableCluster(3);

            // make new index and files
            String newIndexNameOnNodeB = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(
                newIndexNameOnNodeB,
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                    .put("index.routing.allocation.require._name", indexNodeB)
                    .build()
            );
            ensureGreen(newIndexNameOnNodeB);
            indexDocs(newIndexNameOnNodeB, randomIntBetween(1, 10));
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", (String) null), newIndexNameOnNodeB);
        }, (e) -> { assert false : e; }));

        // ensure that the translog files of the new index were not deleted
        var finalTranslogFiles = objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet();
        assertThat(finalTranslogFiles, not(containsInAnyOrder(initialTranslogFiles)));
        assertThat(finalTranslogFiles, is(not(empty())));
    }

    public void testStaleTranslogsAreNotCleanedWhenGCIsDisabled() throws Exception {
        Settings noGCSettings = Settings.builder().put(ObjectStoreGCTask.STALE_TRANSLOGS_GC_ENABLED_SETTING.getKey(), false).build();
        startMasterOnlyNode();
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();

        String indexNodeA = startIndexNode(noGCSettings);
        ensureStableCluster(2);
        String ephemeralIdA = internalCluster().getInstance(ClusterService.class, indexNodeA).localNode().getEphemeralId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        String indexNodeB = startIndexNode(noGCSettings);
        ensureStableCluster(3);

        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);
        int numMoreDocs = randomIntBetween(10, 20);
        numDocs += numMoreDocs;
        indexDocsAndRefresh(indexName, numMoreDocs);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));

        cleanStaleTranslogs(indexNodeB);
        assertThat(
            "The translog files of node A should not have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdA).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(not(empty()))
        );
    }

    public void testTranslogGCDoesNotDeleteLeftNodeWhenRecovering() throws Exception {
        startMasterOnlyNode();
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
        String indexNodeA = startIndexNode(); // has persistent task
        ensureStableCluster(2);

        String indexNodeB = startIndexNode(); // will leave cluster
        ensureStableCluster(3);
        String ephemeralIdB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put("index.routing.allocation.exclude._name", indexNodeA)
                .build()
        );
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        String indexNodeC = startIndexNode(); // will recover shard after node B leaves
        ensureStableCluster(4);

        ObjectStoreService objectStoreServiceC = getObjectStoreService(indexNodeB);
        MockRepository repositoryC = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreServiceC);
        repositoryC.setBlockOnAnyFiles(); // recoveries will not progress

        internalCluster().stopNode(indexNodeB);
        ensureStableCluster(3);
        ClusterHealthStatus health = clusterAdmin().health(new ClusterHealthRequest(TEST_REQUEST_TIMEOUT)).get().getStatus();
        assertThat("health should be RED", health, equalTo(ClusterHealthStatus.RED));

        cleanStaleTranslogs(indexNodeA);
        assertThat(
            "The translog files of node B should not have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(not(empty()))
        );
        repositoryC.unblock();
        ensureGreen(indexName);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));

        cleanStaleTranslogs(indexNodeA);
        assertThat(
            "The translog files of node B should have been deleted",
            objectStoreService.getTranslogBlobContainer(ephemeralIdB).listBlobs(OperationPurpose.TRANSLOG).keySet(),
            is(empty())
        );
    }

    public void testTranslogGCRespectsFileLimit() throws Exception {
        startMasterOnlyNode();
        ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
        Settings settings = Settings.builder()
            .put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
            .put(ObjectStoreGCTask.STALE_TRANSLOGS_GC_FILES_LIMIT_SETTING.getKey(), 1)
            .build();

        String indexNodeA = startIndexNode(settings); // holds persistent GC task
        ensureStableCluster(2);
        AtomicInteger nodes = new AtomicInteger(2);

        String indexNodeB = buildNodeWithIndex(nodes, settings);
        String ephemeralB = internalCluster().getInstance(ClusterService.class, indexNodeB).localNode().getEphemeralId();
        internalCluster().stopNode(indexNodeB);
        nodes.decrementAndGet();
        ensureGreen(indexNodeB);

        String indexNodeC = buildNodeWithIndex(nodes, settings);
        String ephemeralC = internalCluster().getInstance(ClusterService.class, indexNodeC).localNode().getEphemeralId();
        internalCluster().stopNode(indexNodeC);
        nodes.decrementAndGet();
        ensureGreen(indexNodeC);

        assertThat(objectStoreService.getTranslogBlobContainer(ephemeralB).listBlobs(OperationPurpose.TRANSLOG).keySet(), is(not(empty())));
        assertThat(objectStoreService.getTranslogBlobContainer(ephemeralC).listBlobs(OperationPurpose.TRANSLOG).keySet(), is(not(empty())));

        cleanStaleTranslogs(indexNodeA);
        var filesB = objectStoreService.getTranslogBlobContainer(ephemeralB).listBlobs(OperationPurpose.TRANSLOG).keySet();
        var filesC = objectStoreService.getTranslogBlobContainer(ephemeralC).listBlobs(OperationPurpose.TRANSLOG).keySet();
        assertTrue("Only one of the nodes should have been cleaned", filesB.isEmpty() ^ filesC.isEmpty());

        cleanStaleTranslogs(indexNodeA);
        assertThat(objectStoreService.getTranslogBlobContainer(ephemeralB).listBlobs(OperationPurpose.TRANSLOG).keySet(), is(empty()));
        assertThat(objectStoreService.getTranslogBlobContainer(ephemeralC).listBlobs(OperationPurpose.TRANSLOG).keySet(), is(empty()));
    }

    private String buildNodeWithIndex(AtomicInteger nodes, Settings settings) {
        String indexNode = startIndexNode(settings);
        ensureStableCluster(nodes.incrementAndGet());
        createIndex(
            indexNode,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put("index.routing.allocation.require._name", indexNode)
                .build()
        );
        ensureGreen(indexNode);
        indexDocs(indexNode, randomIntBetween(1, 5));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", (String) null), indexNode);
        return indexNode;
    }
}
