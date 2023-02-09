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

import co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.ObjectStoreService.BUCKET_SETTING;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.AZURE;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.FS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.GCS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.S3;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;

public class ObjectStoreServiceTests extends ESTestCase {
    public void testNoBucket() {
        ObjectStoreType type = randomFrom(ObjectStoreType.values());
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE_SETTING.get(builder.build()));
    }

    public void testObjectStoreSettingsNoClient() {
        ObjectStoreType type = randomFrom(S3, GCS, AZURE);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        builder.put(BUCKET_SETTING.getKey(), randomAlphaOfLength(5));
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE_SETTING.get(builder.build()));
    }

    public void testFSSettings() {
        String bucket = randomAlphaOfLength(5);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), FS.name());
        builder.put(BUCKET_SETTING.getKey(), bucket);
        // no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.repositorySettings(bucket, null);
        assertThat(settings.keySet().size(), Matchers.equalTo(1));
        assertThat(settings.get("location"), Matchers.equalTo(bucket));
    }

    public void testObjectStoreSettings() {
        validateObjectStoreSettings(S3, "bucket");
        validateObjectStoreSettings(GCS, "bucket");
        validateObjectStoreSettings(AZURE, "container");
    }

    private void validateObjectStoreSettings(ObjectStoreType type, String bucketName) {
        String bucket = randomAlphaOfLength(5);
        String client = randomAlphaOfLength(5);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        builder.put(BUCKET_SETTING.getKey(), bucket);
        builder.put(ObjectStoreService.CLIENT_SETTING.getKey(), client);
        // check no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.repositorySettings(bucket, client);
        assertThat(settings.keySet().size(), Matchers.equalTo(2));
        assertThat(settings.get(bucketName), Matchers.equalTo(bucket));
        assertThat(settings.get("client"), Matchers.equalTo(client));
    }

    public void testBlobUploadFailureBlocksSegmentsUpload() throws IOException {
        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        final var permits = new Semaphore(0);
        try (var testHarness = new TestHarness() {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                        throws IOException {
                        assertFalse(blobName, blobName.startsWith(IndexFileNames.SEGMENTS));
                        if (permits.tryAcquire()) {
                            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
                        } else {
                            throw new IOException("simulated upload failure");
                        }
                    }

                    @Override
                    public void writeMetadataBlob(
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) {
                        throw new AssertionError("should not be called");
                    }

                    @Override
                    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
                        throw new AssertionError("should not be called");
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        }; var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {

            var commits = between(1, 3);
            for (int i = 0; i < commits; i++) {
                indexWriter.addDocument(List.of());
                indexWriter.commit();
            }

            var primaryTerm = randomNonNegativeLong();

            try (var indexReader = DirectoryReader.open(indexWriter)) {

                final var indexCommit = indexReader.getIndexCommit();
                final var commitFiles = testHarness.indexingStore.getMetadata(indexCommit).fileMetadataMap();

                final var permittedBlobs = between(0, commitFiles.size() - 2);
                permits.release(permittedBlobs);

                PlainActionFuture.<Void, IOException>get(
                    future -> testHarness.objectStoreService.onCommitCreation(
                        new StatelessCommitRef(
                            testHarness.shardId,
                            new Engine.IndexCommitRef(indexCommit, () -> future.onResponse(null)),
                            commitFiles,
                            commitFiles.keySet(),
                            primaryTerm
                        )
                    ),
                    10,
                    TimeUnit.SECONDS
                );

                final var blobs = new HashSet<>(
                    testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm).listBlobs().keySet()
                );
                blobs.removeIf(ExtrasFS::isExtra);
                assertEquals(permittedBlobs + " vs " + blobs, permittedBlobs, blobs.size());
                for (String blobName : blobs) {
                    assertFalse(blobName, blobName.startsWith(IndexFileNames.SEGMENTS));
                }
            }
        }
    }

    public void testStartingShardRetrievesSegmentsFromOneCommit() throws IOException {
        final var mergesEnabled = randomBoolean();
        final var indexWriterConfig = mergesEnabled
            ? new IndexWriterConfig(new KeywordAnalyzer())
            : Lucene.indexWriterConfigWithNoMerging(new KeywordAnalyzer());

        final var permittedFiles = new HashSet<String>();
        try (var testHarness = new TestHarness() {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public InputStream readBlob(String blobName) throws IOException {
                        assert permittedFiles.contains(blobName) : blobName + " in " + permittedFiles;
                        return super.readBlob(blobName);
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        }) {
            var commitCount = between(0, 5);

            try (
                var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig);
                var closeables = new TransferableCloseables()
            ) {

                for (int commit = 0; commit < commitCount; commit++) {
                    indexWriter.addDocument(List.of());
                    indexWriter.forceMerge(1);
                    indexWriter.commit();
                    final var indexReader = closeables.add(DirectoryReader.open(indexWriter));
                    final var indexCommit = indexReader.getIndexCommit();
                    final var commitFiles = testHarness.indexingStore.getMetadata(indexCommit).fileMetadataMap();
                    if (commit == 0 || mergesEnabled == false) {
                        final var segmentCommitInfos = SegmentInfos.readCommit(
                            testHarness.indexingDirectory,
                            indexCommit.getSegmentsFileName()
                        );
                        assertEquals(commit + 1, segmentCommitInfos.size());
                        for (SegmentCommitInfo segmentCommitInfo : segmentCommitInfos) {
                            assertTrue(segmentCommitInfo.info.getUseCompoundFile());
                        }
                    }

                    permittedFiles.clear();
                    permittedFiles.addAll(indexCommit.getFileNames());

                    PlainActionFuture.<Void, IOException>get(
                        future -> testHarness.objectStoreService.onCommitCreation(
                            new StatelessCommitRef(
                                testHarness.shardId,
                                new Engine.IndexCommitRef(indexCommit, () -> future.onResponse(null)),
                                commitFiles,
                                commitFiles.keySet(),
                                1
                            )
                        ),
                        10,
                        TimeUnit.SECONDS
                    );
                }
            }

            assertEquals(
                commitCount,
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId, 1)
                    .listBlobs()
                    .keySet()
                    .stream()
                    .filter(s -> s.startsWith(IndexFileNames.SEGMENTS))
                    .count()
            );
            assertEquals(Math.min(1L, commitCount), permittedFiles.stream().filter(s -> s.startsWith(IndexFileNames.SEGMENTS)).count());

            final var dir = SearchDirectory.unwrapDirectory(testHarness.searchStore.directory());
            final var blobContainer = testHarness.objectStoreService.getBlobContainer(testHarness.shardId, 1);
            dir.init(blobContainer);

            if (commitCount > 0) {
                assertEquals(permittedFiles, Set.of(dir.listAll()));
            }

            try (var indexReader = DirectoryReader.open(testHarness.searchStore.directory())) {
                assertEquals(commitCount, indexReader.numDocs());
            }
        }
    }

    private class TestHarness implements Closeable {

        final DiscoveryNode node;
        final Path repoPath;
        final Settings nodeSettings;
        final ClusterSettings clusterSettings;
        final Environment environment;
        final IndexMetadata indexMetadata;
        final ShardId shardId;
        final IndexSettings indexSettings;
        final MockTransport transport;
        final ClusterService clusterService;
        final NodeClient client;
        final ShardPath indexingShardPath;
        final Directory indexingDirectory;
        final Store indexingStore;
        final ShardPath searchShardPath;
        final Directory searchDirectory;
        final Store searchStore;
        final TransportService transportService;
        final RepositoriesService repoService;
        final ObjectStoreService objectStoreService;

        private final Closeable closeables;

        TestHarness() throws IOException {
            node = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
            repoPath = createTempDir();
            nodeSettings = Settings.builder().put(PATH_REPO_SETTING.getKey(), repoPath).put(BUCKET_SETTING.getKey(), repoPath).build();
            clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            environment = newEnvironment(nodeSettings);

            indexMetadata = IndexMetadata.builder("index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                )
                .build();
            shardId = new ShardId(indexMetadata.getIndex(), 0);
            indexSettings = new IndexSettings(indexMetadata, nodeSettings);
            indexingShardPath = new ShardPath(
                false,
                createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
                createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
                shardId
            );
            searchShardPath = new ShardPath(
                false,
                createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
                createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
                shardId
            );

            try (var localCloseables = new TransferableCloseables()) {

                final var threadPool = new TestThreadPool("test");
                localCloseables.add(() -> TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

                transport = localCloseables.add(new MockTransport());
                clusterService = localCloseables.add(ClusterServiceUtils.createClusterService(threadPool));
                client = localCloseables.add(new NodeClient(nodeSettings, threadPool));
                indexingDirectory = localCloseables.add(new FsDirectoryFactory().newDirectory(indexSettings, indexingShardPath));
                indexingStore = localCloseables.add(new Store(shardId, indexSettings, indexingDirectory, new DummyShardLock(shardId)));
                final var nodeEnv = newNodeEnvironment(nodeSettings);
                localCloseables.add(nodeEnv);
                final var sharedCacheService = new SharedBlobCacheService<FileCacheKey>(nodeEnv, nodeSettings, threadPool);
                localCloseables.add(sharedCacheService);
                searchDirectory = localCloseables.add(new SearchDirectory(sharedCacheService, searchShardPath.getShardId()));
                searchStore = localCloseables.add(new Store(shardId, indexSettings, searchDirectory, new DummyShardLock(shardId)));

                transportService = transport.createTransportService(
                    nodeSettings,
                    threadPool,
                    TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                    ignored -> node,
                    null,
                    Set.of()
                );

                repoService = new RepositoriesService(
                    nodeSettings,
                    clusterService,
                    transportService,
                    Map.of(
                        FsRepository.TYPE,
                        metadata -> new FsRepository(
                            metadata,
                            environment,
                            xContentRegistry(),
                            clusterService,
                            BigArrays.NON_RECYCLING_INSTANCE,
                            new RecoverySettings(nodeSettings, clusterSettings)
                        ) {
                            @Override
                            protected BlobStore createBlobStore() throws Exception {
                                final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
                                final Path locationFile = environment.resolveRepoFile(location);
                                return new FsBlobStore(bufferSize, locationFile, isReadOnly()) {
                                    @Override
                                    public BlobContainer blobContainer(BlobPath path) {
                                        return wrapBlobContainer(path, super.blobContainer(path));
                                    }
                                };
                            }
                        }
                    ),
                    Map.of(),
                    threadPool,
                    List.of()
                );

                transportService.start();
                transportService.acceptIncomingRequests();
                localCloseables.add(transportService::stop);

                objectStoreService = new ObjectStoreService(nodeSettings, () -> repoService, threadPool, clusterService, client);
                objectStoreService.start();

                closeables = localCloseables.transfer();
            }
        }

        public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
            return innerContainer;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(closeables);
        }
    }

    /**
     * Encapsulates a common pattern of trying to open a bunch of resources and then transferring ownership elsewhere on success,
     * but closing them on failure.
     */
    private static class TransferableCloseables implements Closeable {

        private boolean transferred = false;
        private final List<Closeable> closeables = new ArrayList<>();

        <T extends Closeable> T add(T releasable) {
            assert transferred == false : "already transferred";
            closeables.add(releasable);
            return releasable;
        }

        Closeable transfer() {
            assert transferred == false : "already transferred";
            transferred = true;
            Collections.reverse(closeables);
            return () -> IOUtils.close(closeables);
        }

        @Override
        public void close() throws IOException {
            if (transferred == false) {
                IOUtils.close(closeables);
            }
        }
    }
}
