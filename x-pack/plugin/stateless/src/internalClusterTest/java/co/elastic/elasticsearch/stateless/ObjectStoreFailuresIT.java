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

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ObjectStoreFailuresIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testRecoverIndexingShardWithObjectStoreFailuresDuringIndexing() throws Exception {
        startMasterOnlyNode();
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        startIndexNode();
        ensureStableCluster(3);

        ObjectStoreService objectStoreService = getObjectStoreService(indexNodeA);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
        // This pattern starts failing from file 10. Because file name has 19 digits, and final digit should not be preceded by 18 zeroes.
        repository.setRandomIOExceptionPattern(".*translog/\\d{18,18}(?<!000000000000000000)\\d.*");

        final AtomicInteger docIdGenerator = new AtomicInteger();
        final AtomicInteger docsAcknowledged = new AtomicInteger();
        final AtomicInteger docsFailed = new AtomicInteger();
        final IntConsumer docIndexer = numDocs -> {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < numDocs; i++) {
                bulkRequest.add(
                    new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                        .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                );
            }
            BulkResponse response = bulkRequest.get(TimeValue.timeValueSeconds(15));
            assertThat(response.getItems().length, equalTo(numDocs));
            for (BulkItemResponse itemResponse : response.getItems()) {
                if (itemResponse.isFailed()) {
                    docsFailed.incrementAndGet();
                } else {
                    docsAcknowledged.incrementAndGet();
                }
            }
        };

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(() -> {
                while (running.get()) {
                    docIndexer.accept(between(1, 20));
                }
            });
            threads[j].start();
        }

        try {
            assertBusy(() -> assertThat(repository.getFailureCount(), greaterThan(0L)));
        } finally {
            running.set(false);
            internalCluster().stopNode(indexNodeA);
            for (Thread thread : threads) {
                thread.join();
            }
        }

        logger.info("--> [{}] documents acknowledged, [{}] documents failed", docsAcknowledged, docsFailed);
        ensureGreen();

        refresh(indexName); // so that any translog ops become visible for searching
        final long totalHits = SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName));
        assertThat(totalHits, greaterThanOrEqualTo((long) docsAcknowledged.get()));
    }

    public void testRecoverSearchShardWithObjectStoreFailures() throws Exception {
        startMasterOnlyNode();
        final String indexName = "test";
        startIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(25, 250);
        indexDocsAndRefresh(indexName, numDocs);
        ensureSearchable(indexName);

        ObjectStoreService objectStoreService = getObjectStoreService(searchNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) repository.setRandomIOExceptionPattern(".*stateless_commit_.*");

        logger.info("--> starting search shard");
        setReplicaCount(1, indexName);

        ensureGreen();
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertHitCount(prepareSearch(indexName), numDocs);
    }

    public void testRelocateSearchShardWithObjectStoreFailures() throws Exception {
        startMasterOnlyNode();
        final String indexName = "test";
        startIndexNode();
        final String searchNodeA = startSearchNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 1).build());
        int numDocs = scaledRandomIntBetween(25, 250);
        indexDocsAndRefresh(indexName, numDocs);
        final String searchNodeB = startSearchNode();
        ensureStableCluster(4);

        ObjectStoreService objectStoreService = getObjectStoreService(searchNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) repository.setRandomIOExceptionPattern(".*stateless_commit_.*");

        logger.info("--> move replica shard from: {} to: {}", searchNodeA, searchNodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, searchNodeA, searchNodeB));

        ensureGreen(indexName);
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertNodeHasNoCurrentRecoveries(searchNodeB);
        assertThat(findSearchShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(searchNodeB)));
        assertHitCount(client(searchNodeB).prepareSearch(indexName).setPreference("_local"), numDocs);
    }

    public void testRecoverIndexingShardWithObjectStoreFailures() throws Exception {
        startMasterOnlyNode();
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        ObjectStoreService objectStoreService = getObjectStoreService(indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);
        if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*stateless_commit_.*");
        } else if (randomBoolean()) {
            repository.setRandomIOExceptionPattern(".*translog.*");
        }

        logger.info("--> stopping node [{}]", indexNodeA);
        internalCluster().stopNode(indexNodeA);
        ensureStableCluster(2);

        ensureGreen();
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    public void testRelocateIndexingShardWithObjectStoreFailures() {
        startMasterOnlyNode();
        final String indexNodeA = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        boolean failuresOnSource = randomBoolean(); // else failures on target node
        logger.info("--> failures will be on source node? [{}]", failuresOnSource);
        ObjectStoreService objectStoreService = getObjectStoreService(failuresOnSource ? indexNodeA : indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(1);

        logger.info("--> failures will be on stateless commits");
        repository.setRandomIOExceptionPattern(".*stateless_commit_.*");

        logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB));

        ensureGreen(indexName);
        assertThat(repository.getFailureCount(), greaterThan(0L));
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }
}
