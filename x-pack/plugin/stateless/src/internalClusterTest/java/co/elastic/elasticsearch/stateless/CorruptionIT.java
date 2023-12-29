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

import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.createFullSnapshot;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * An IT aiming at reproducing corruption-related issues such as:
 * - https://elasticco.atlassian.net/browse/ES-7154
 * - https://elasticco.atlassian.net/browse/ES-7210
 *
 * The test run can be adjusted by using `-Dtests.harder=true` which:
 * - Increases the number and size of the indexing bulk requests
 * - Increases the number of shards.
 * - Increases the frequency of force merges
 * - Increases the frequency of moving shards around
 * - Increases the number of indexers and searchers
 * - Increases the frequency of full cluster snapshots
 * - Uses network delays between the nodes
 *
 * TODO: Should we have a test for data streams or randomly use an index or a data stream?
 * TODO: Increase network delay disruption and maybe use other NetworkDisruption types.
 * TODO: Add random object store failures beyond max retries (https://elasticco.atlassian.net/browse/ES-6453)
 */
public class CorruptionIT extends AbstractStatelessIntegTestCase {

    private static final boolean TEST_HARDER = RandomizedTest.systemPropertyAsBoolean("tests.harder", false);

    private final int MAX_BULK_SIZE = TEST_HARDER ? 500 : 50;
    private final int MAX_DOCS_PER_BULK = TEST_HARDER ? 100 : 10;
    private final int MAX_SHARDS = TEST_HARDER ? 50 : 3;
    private final int MAX_SEARCHERS_AND_INDEXERS = TEST_HARDER ? 20 : 3;
    private final Tuple<Long, Long> FORCE_MERGE_SLEEP = TEST_HARDER ? tuple(10L, 1_000L) : tuple(2_000L, 5_000L);
    private final Tuple<Long, Long> ALLOCATION_UPDATE_SLEEP = TEST_HARDER ? tuple(0L, 500L) : tuple(2_000L, 5_000L);
    private final Tuple<Long, Long> SNAPSHOT_SLEEP = TEST_HARDER ? tuple(500L, 2000L) : tuple(5_000L, 10_000L);
    private final boolean NETWORK_DISRUPTIONS = TEST_HARDER ? true : false;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(SlowRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    /**
     * Run indexing, searching, force merge and snapshotting operations on the cluster while
     * moving indexing and search shards around.
     */
    @LuceneTestCase.AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-6563")
    public void testConcurrentOperations() throws Exception {
        startMasterOnlyNode();
        var smallCache = randomBoolean();
        var settings = smallCache
            ? Settings.builder()
                .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), randomFrom("8kb", "16kb"))
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "32kb")
                .build()
            : Settings.EMPTY;
        var searchNodes = List.of(startSearchNode(settings), startSearchNode(settings));
        var indexNodes = List.of(startIndexNode(settings), startIndexNode(settings));
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(randomIntBetween(1, MAX_SHARDS), 1)
                // ES-7154 and ES-7210 manifest as transient `CorruptIndexException`s that fail shard recovery
                // but could be resolved when the recovery is retried, therefore we disable allocation retries.
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(timeValueSeconds(TEST_HARDER ? 90 : 30), indexName);

        ServiceDisruptionScheme networkDisruption = null;
        if (NETWORK_DISRUPTIONS) {
            networkDisruption = addNetworkDisruptions();
            networkDisruption.startDisrupting();
        }

        var stop = new AtomicBoolean();
        var numberOfSearchers = randomIntBetween(1, MAX_SEARCHERS_AND_INDEXERS);
        var numberOfIndexers = randomIntBetween(1, MAX_SEARCHERS_AND_INDEXERS);
        var indexersRunning = new CountDownLatch(numberOfIndexers);
        Runnable indexer = () -> {
            try {
                var bulks = randomIntBetween(5, MAX_BULK_SIZE);
                Set<String> existingDocIds = new HashSet<>();
                for (int bulk = 0; bulk < bulks & stop.get() == false; bulk++) {
                    var bulkRequest = client().prepareBulk();
                    var docs = randomIntBetween(1, MAX_DOCS_PER_BULK);
                    IntStream.range(0, docs)
                        .forEach(
                            i -> bulkRequest.add(
                                new IndexRequest(indexName).source("field", randomUnicodeOfLengthBetween(100, 1024 * 1024))
                            )
                        );
                    if (randomBoolean() && false) { // Do not update to avoid hitting https://elasticco.atlassian.net/browse/ES-6563
                        var updateCount = Math.min(existingDocIds.size(), randomIntBetween(1, MAX_DOCS_PER_BULK / 2));
                        var idsToUpdate = randomSubsetOf(updateCount, existingDocIds);
                        idsToUpdate.forEach(
                            id -> bulkRequest.add(
                                new UpdateRequest(indexName, id).doc("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                            )
                        );
                    }
                    Set<String> idsToDelete = new HashSet<>();
                    if (randomBoolean() && false) { // Do not delete to avoid hitting https://elasticco.atlassian.net/browse/ES-6563
                        var deleteCount = Math.min(existingDocIds.size(), randomIntBetween(1, MAX_DOCS_PER_BULK / 2));
                        idsToDelete = new HashSet<>(randomSubsetOf(deleteCount, existingDocIds));
                        idsToDelete.forEach(id -> bulkRequest.add(new DeleteRequest(indexName, id)));
                    }
                    bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE)).setTimeout(timeValueSeconds(60));
                    var bulkResponse = bulkRequest.get();
                    assertNoFailures(bulkResponse);
                    var ids = Arrays.stream(bulkResponse.getItems()).map(r -> r.getResponse().getId()).toList();
                    existingDocIds.addAll(ids);
                    existingDocIds.removeAll(idsToDelete);
                    safeSleep(randomLongBetween(0, 500));
                }
            } catch (Throwable e) {
                stop.set(true);
                throw new AssertionError(e);
            } finally {
                indexersRunning.countDown();
            }
        };

        Runnable updateAllocation = () -> {
            var searchNodeRequired = randomFrom(searchNodes);
            var indexNodeRequired = randomFrom(indexNodes);
            try {
                while (indexersRunning.getCount() > 0 && stop.get() == false) {
                    if (randomBoolean()) {
                        searchNodeRequired = searchNodeRequired.equals(searchNodes.get(0)) ? searchNodes.get(1) : searchNodes.get(0);
                        logger.info("--> Moving search shard to {}", searchNodeRequired);
                        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", searchNodeRequired));
                    }
                    if (randomBoolean()) {
                        indexNodeRequired = indexNodeRequired.equals(indexNodes.get(0)) ? indexNodes.get(1) : indexNodes.get(0);
                        logger.info("--> Moving index shard to {}", indexNodeRequired);
                        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodeRequired));
                    }
                    ensureGreen(timeValueSeconds(TEST_HARDER ? 90 : 30), indexName);
                    safeSleep(randomLongBetween(ALLOCATION_UPDATE_SLEEP.v1(), ALLOCATION_UPDATE_SLEEP.v2()));
                }
            } catch (Throwable e) {
                stop.set(true);
                throw new AssertionError(e);
            }
        };

        Runnable searcher = () -> {
            try {
                while (indexersRunning.getCount() > 0 && stop.get() == false) {
                    assertNoFailures(client().prepareSearch(indexName).setTimeout(timeValueSeconds(60)).get());
                    safeSleep(randomLongBetween(100, 1_000));
                }
            } catch (Throwable e) {
                stop.set(true);
                throw new AssertionError(e);
            }
        };

        Runnable forceMerge = () -> {
            try {
                while (indexersRunning.getCount() > 0 && stop.get() == false) {
                    if (randomBoolean()) {
                        logger.info("--> Force merging");
                        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).get());
                    }
                    safeSleep(randomLongBetween(FORCE_MERGE_SLEEP.v1(), FORCE_MERGE_SLEEP.v2()));
                }
            } catch (Throwable e) {
                stop.set(true);
                throw new AssertionError(e);
            }
        };

        if (smallCache) {
            var repoSettings = Settings.builder()
                .put(BlobStoreRepository.BUFFER_SIZE_SETTING.getKey(), "8k")
                .put("location", randomRepoPath())
                .put("compress", randomBoolean());
            createRepository(logger, "test-repo", "fs", repoSettings, true);
        } else {
            createRepository(logger, "test-repo", "fs");
        }
        Runnable snapshot = () -> {
            try {
                var snapshotId = 0;
                while (indexersRunning.getCount() > 0 && stop.get() == false) {
                    createFullSnapshot(logger, "test-repo", "test-snap-" + snapshotId++);
                    safeSleep(randomLongBetween(SNAPSHOT_SLEEP.v1(), SNAPSHOT_SLEEP.v2()));
                }
            } catch (Throwable e) {
                stop.set(true);
                throw new AssertionError(e);
            }
        };

        var threads = new ArrayList<Thread>();
        threads.add(new Thread(updateAllocation));
        threads.add(new Thread(forceMerge));
        threads.add(new Thread(snapshot));
        for (int i = 0; i < numberOfIndexers; i++) {
            threads.add(new Thread(indexer));
        }
        for (int i = 0; i < numberOfSearchers; i++) {
            threads.add(new Thread(searcher));
        }
        threads.forEach(Thread::start);
        for (var thread : threads) {
            thread.join();
        }
        createFullSnapshot(logger, "test-repo", "test-snap-final");
        if (NETWORK_DISRUPTIONS) {
            networkDisruption.stopDisrupting();
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1301")
    public void testWillEvictCacheOnCorruptionError() throws Exception {
        final String indexNodeName = startMasterAndIndexNode();
        final String searchNodeName = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        for (int i = 0; i < between(3, 5); i++) {
            indexDocs(indexName, between(5, 10));
            refresh(indexName);
        }

        // search node
        assertCacheEvictedOnCorruption(
            findSearchShard(indexName),
            (TestSharedBlobCacheService) internalCluster().getInstance(Stateless.SharedBlobCacheServiceSupplier.class, searchNodeName).get()
        );

        // index node
        assertCacheEvictedOnCorruption(
            findIndexShard(indexName),
            (TestSharedBlobCacheService) internalCluster().getInstance(Stateless.SharedBlobCacheServiceSupplier.class, indexNodeName).get()
        );
    }

    private void assertCacheEvictedOnCorruption(IndexShard shard, TestSharedBlobCacheService sharedBlobCacheService) throws Exception {
        Exception e = new CorruptIndexException("test", shard.toString());
        if (randomBoolean()) {
            e = new IOException("io error", e);
        }

        shard.failShard("test", e);

        assertBusy(() -> {
            assertThat(sharedBlobCacheService.cacheKeyPredicates.size(), equalTo(1));
            assertThat(
                sharedBlobCacheService.cacheKeyPredicates.get(0)
                    .test(new FileCacheKey(shard.shardId(), randomNonNegativeLong(), randomAlphaOfLengthBetween(3, 15))),
                is(true)
            );
        });
    }

    private ServiceDisruptionScheme addNetworkDisruptions() {
        final NetworkDisruption.DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = NetworkDisruption.TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = NetworkDisruption.Bridge.random(random(), internalCluster().getNodeNames());
        }
        var scheme = new NetworkDisruption(
            disruptedLinks,
            NetworkDisruption.NetworkDelay.random(random(), timeValueSeconds(1), timeValueSeconds(10))
        );
        internalCluster().setDisruptionScheme(scheme);
        return scheme;
    }

    public static class TestStateless extends Stateless {
        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheService<FileCacheKey> createSharedBlobCacheService(
            PluginServices services,
            NodeEnvironment nodeEnvironment,
            Settings settings,
            ThreadPool threadPool
        ) {
            return new TestSharedBlobCacheService(
                nodeEnvironment,
                settings,
                threadPool,
                SHARD_READ_THREAD_POOL,
                new BlobCacheMetrics(services.telemetryProvider().getMeterRegistry())
            );
        }
    }

    public static class TestSharedBlobCacheService extends SharedBlobCacheService<FileCacheKey> {

        List<Predicate<FileCacheKey>> cacheKeyPredicates = Collections.synchronizedList(new ArrayList<>());

        public TestSharedBlobCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            String ioExecutor,
            BlobCacheMetrics blobCacheMetrics
        ) {
            super(environment, settings, threadPool, ioExecutor, blobCacheMetrics);
        }

        @Override
        public int forceEvict(Predicate<FileCacheKey> cacheKeyPredicate) {
            cacheKeyPredicates.add(cacheKeyPredicate);
            return super.forceEvict(cacheKeyPredicate);
        }
    }

    public static class SlowRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = ObjectStoreService.ObjectStoreType.MOCK.toString().toLowerCase(Locale.ROOT);

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new SlowMockRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    public static class SlowMockRepository extends MockRepository {

        private final Tuple<Long, Long> OBJECT_STORE_LATENCY = TEST_HARDER ? tuple(0L, 2000L) : tuple(0L, 10L);

        public SlowMockRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new SlowBlobStore(super.createBlobStore());
        }

        private void maybeSleep(String blobName) {
            if (randomBoolean()) {
                safeSleep(randomLongBetween(OBJECT_STORE_LATENCY.v1(), OBJECT_STORE_LATENCY.v2()));
            }
        }

        private class SlowBlobStore extends BlobStoreWrapper {

            SlowBlobStore(BlobStore delegate) {
                super(delegate);
            }

            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new SlowBlobContainer(super.blobContainer(path));
            }
        }

        private class SlowBlobContainer extends FilterBlobContainer {

            SlowBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new SlowBlobContainer(child);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                maybeSleep(blobName);
                return super.readBlob(purpose, blobName);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                maybeSleep(blobName);
                return super.readBlob(purpose, blobName, position, length);
            }

            @Override
            public void writeBlob(
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                maybeSleep(blobName);
                super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }

            @Override
            public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
                throws IOException {
                maybeSleep(blobName);
                super.writeBlob(purpose, blobName, bytes, failIfAlreadyExists);
            }

            @Override
            public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
                maybeSleep("");
                return super.listBlobs(purpose);
            }

            @Override
            public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
                maybeSleep("");
                return super.listBlobsByPrefix(purpose, blobNamePrefix);
            }
        }
    }
}
