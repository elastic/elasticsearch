/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbortedRestoreIT.BlockingDataFileReadsRepository.BlockingInputStream;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AbortedRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(BlockingDataFileReadsRepository.Plugin.class);
        return plugins;
    }

    public void testAbortedRestoreAlsoAbortFileRestores() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String indexName = "test-abort-restore";
        final int numPrimaries = randomIntBetween(1, 3);
        createIndex(indexName, indexSettingsNoReplicas(numPrimaries).build());
        indexRandomDocs(indexName, scaledRandomIntBetween(10, 1_000));
        ensureGreen();
        forceMerge();

        final String repositoryName = "repository";
        createRepository(repositoryName, BlockingDataFileReadsRepository.TYPE, Settings.builder().put("location", randomRepoPath()));

        final String snapshotName = "snapshot";
        createFullSnapshot(repositoryName, snapshotName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> blocking node on data files [{}] before restore", dataNode);
        final BlockingDataFileReadsRepository blockRepository = ((BlockingDataFileReadsRepository) internalCluster()
            .getInstance(RepositoriesService.class, dataNode)
            .repository(repositoryName));
        blockRepository.block();

        logger.info("--> starting restore");
        final ActionFuture<RestoreSnapshotResponse> future = client().admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .execute();

        assertBusy(() -> {
            final RecoveryResponse recoveries = client().admin().indices().prepareRecoveries(indexName)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN).setActiveOnly(true).get();
            assertThat(recoveries.hasRecoveries(), is(true));
            final List<RecoveryState> shardRecoveries = recoveries.shardRecoveryStates().get(indexName);
            assertThat(shardRecoveries, hasSize(numPrimaries));
            assertThat(future.isDone(), is(false));

            for (RecoveryState shardRecovery : shardRecoveries) {
                assertThat(shardRecovery.getRecoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));
                assertThat(shardRecovery.getStage(), equalTo(RecoveryState.Stage.INDEX));
            }
        });

        logger.info("--> waiting for snapshot thread pool to be full");
        assertBusy(() -> {
            ThreadPool threadPool = internalCluster().getInstance(ClusterService.class, dataNode).getClusterApplierService().threadPool();
            int activeSnapshotThreads = -1;
            for (ThreadPoolStats.Stats threadPoolStats : threadPool.stats()) {
                if (threadPoolStats.getName().equals(ThreadPool.Names.SNAPSHOT)) {
                    activeSnapshotThreads = threadPoolStats.getActive();
                    break;
                }
            }
            final ThreadPool.Info threadPoolInfo = threadPool.info(ThreadPool.Names.SNAPSHOT);
            assertThat(activeSnapshotThreads, allOf(greaterThan(0), equalTo(threadPoolInfo.getMax())));
            assertThat(blockRepository.streams().filter(BlockingInputStream::isBlocked).count(), equalTo((long) activeSnapshotThreads));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> aborting restore by deleting the index");
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // Total number of blobs that have been opened from the blob store
        final long totalBlobsRead = blockRepository.streams().count();

        // Total number of bytes that have been read from the blob store
        final long totalBytesRead = blockRepository.streams().mapToLong(BlockingInputStream::getCount).sum();

        logger.info("--> unblocking node [{}]", dataNode);
        blockRepository.unblock();
        assertThat(blockRepository.streams().noneMatch(BlockingInputStream::isBlocked), is(true));

        logger.info("--> restore should have failed");
        final RestoreSnapshotResponse restoreSnapshotResponse = future.get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(numPrimaries));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));

        logger.info("--> waiting for snapshot thread pool to be empty");
        assertBusy(() -> {
            ThreadPool threadPool = internalCluster().getInstance(ClusterService.class, dataNode).getClusterApplierService().threadPool();
            int activeSnapshotThreads = -1;
            for (ThreadPoolStats.Stats threadPoolStats : threadPool.stats()) {
                if (threadPoolStats.getName().equals(ThreadPool.Names.SNAPSHOT)) {
                    activeSnapshotThreads = threadPoolStats.getActive();
                    break;
                }
            }
            assertThat(activeSnapshotThreads, equalTo(0));
            assertThat(blockRepository.streams().filter(BlockingInputStream::isBlocked).count(), equalTo(0L));
        }, 30L, TimeUnit.SECONDS);

        assertThat("No more blobs should have been opened from the blob store",
            blockRepository.streams().count(), equalTo(totalBlobsRead));
        assertThat("No more bytes should have been read from the blob store",
            blockRepository.streams().mapToLong(BlockingInputStream::getCount).sum(), equalTo(totalBytesRead));
    }

    /**
     * A blob store repository that blocks read operations on {@link InputStream} when the {@code blockStreams} flag
     * is set to true. It also keep track of the number of bytes read from {@link InputStream} it opens.
     */
    public static class BlockingDataFileReadsRepository extends FsRepository {

        static final String TYPE = "block_on_data_file_reads";

        private final List<BlockingInputStream> streams = Collections.synchronizedList(new ArrayList<>());
        private volatile boolean blockStreams = false;

        public BlockingDataFileReadsRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        @Override
        protected int bufferSize() {
            if (blockStreams) {
                return randomIntBetween(10, 100); // use a low buffer size when restoring files from blocked streams
            }
            return super.bufferSize();
        }

        private synchronized boolean shouldBlock(String blobName) {
            if (blobName.startsWith("__")) {
                return blockStreams;
            }
            return false;
        }

        private synchronized void block() {
            blockStreams = true;
        }

        private synchronized void unblock() {
            blockStreams = false;
            streams.forEach(BlockingInputStream::unblock);
        }

        private synchronized void addBlockingInputStream(BlockingInputStream stream) {
            streams.add(stream);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new BlockingBlobStore(super.createBlobStore(), this);
        }

        public Stream<BlockingInputStream> streams() {
            return streams.stream();
        }

        public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

            @Override
            public Map<String, Factory> getRepositories(
                Environment env,
                NamedXContentRegistry registry,
                ClusterService clusterService,
                RecoverySettings recoverySettings
            ) {
                return Collections.singletonMap(TYPE, (metadata) ->
                    new BlockingDataFileReadsRepository(metadata, env, registry, clusterService, recoverySettings));
            }
        }

        static class BlockingBlobStore extends BlobStoreWrapper {

            private final BlockingDataFileReadsRepository repository;

            BlockingBlobStore(BlobStore delegate, BlockingDataFileReadsRepository repository) {
                super(delegate);
                this.repository = Objects.requireNonNull(repository);
            }

            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new BlockingBlobContainer(super.blobContainer(path), repository);
            }
        }

        static class BlockingBlobContainer extends FilterBlobContainer {

            private final BlockingDataFileReadsRepository repository;

            BlockingBlobContainer(BlobContainer delegate, BlockingDataFileReadsRepository repository) {
                super(delegate);
                this.repository = Objects.requireNonNull(repository);
            }

            private InputStream createBlockingInputStream(InputStream inputStream) {
                final BlockingInputStream stream = new BlockingInputStream(inputStream);
                repository.addBlockingInputStream(stream);
                return stream;
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new BlockingBlobContainer(child, repository);
            }

            @Override
            public InputStream readBlob(String blobName) throws IOException {
                final InputStream stream = super.readBlob(blobName);
                if (repository.shouldBlock(blobName)) {
                    return createBlockingInputStream(stream);
                }
                return stream;
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                final InputStream stream = super.readBlob(blobName, position, length);
                if (repository.shouldBlock(blobName) == false) {
                    return createBlockingInputStream(stream);
                }
                return stream;
            }
        }

        static class BlockingInputStream extends FilterInputStream {

            private final CountDownLatch latch;
            private long count;

            protected BlockingInputStream(InputStream delegate) {
                super(delegate);
                this.latch = new CountDownLatch(1);
            }

            public boolean isBlocked() {
                return latch.getCount() > 0;
            }

            private void unblock() {
                latch.countDown();
            }

            private void waitForUnblock() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int read() throws IOException {
                final int result = super.read();
                if (result != -1) {
                    count += 1;
                }
                waitForUnblock();
                return result;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final int result = super.read(b, off, len);
                if (result > 0) {
                    count += result;
                }
                waitForUnblock();
                return result;
            }

            public long getCount() {
                return count;
            }
        }
    }
}
