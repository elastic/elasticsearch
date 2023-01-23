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
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
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
        final var node = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        final var repoPath = createTempDir();
        final var nodeSettings = Settings.builder()
            .put(PATH_REPO_SETTING.getKey(), repoPath)
            .put(BUCKET_SETTING.getKey(), repoPath)
            .build();
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var environment = newEnvironment(nodeSettings);

        final var indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();
        final var shardId = new ShardId(indexMetadata.getIndex(), 0);
        final var indexSettings = new IndexSettings(indexMetadata, nodeSettings);
        final var shardPath = new ShardPath(
            false,
            createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            shardId
        );

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setCommitOnClose(true);

        final var threadPool = new TestThreadPool("test");
        try (
            var transport = new MockTransport();
            var clusterService = ClusterServiceUtils.createClusterService(threadPool);
            var client = new NodeClient(nodeSettings, threadPool);
            var directory = new FsDirectoryFactory().newDirectory(indexSettings, shardPath);
            var store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
            var indexWriter = new IndexWriter(store.directory(), indexWriterConfig)
        ) {

            final var transportService = transport.createTransportService(
                nodeSettings,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> node,
                null,
                Set.of()
            );

            final var permits = new Semaphore(0);

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

            final var repoService = new RepositoriesService(
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
                                    return new WrappedBlobContainer(super.blobContainer(path));
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

            try (var objectStoreService = new ObjectStoreService(nodeSettings, () -> repoService, threadPool, client, clusterService)) {
                objectStoreService.start();

                indexWriter.addDocument(List.of());
                indexWriter.commit();

                try (var indexReader = DirectoryReader.open(indexWriter)) {
                    final var indexCommit = indexReader.getIndexCommit();
                    final var commitFiles = store.getMetadata(indexCommit).fileMetadataMap();

                    permits.release(between(0, commitFiles.size() - 2));

                    PlainActionFuture.<Void, IOException>get(
                        fut -> objectStoreService.onCommitCreation(
                            new StatelessCommitRef(
                                shardId,
                                new Engine.IndexCommitRef(indexCommit, () -> fut.onResponse(null)),
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

        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

}
