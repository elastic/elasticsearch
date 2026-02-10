/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating a drop-in {@link Directory} backed by searchable snapshot infrastructure.
 *
 * <p> Usage:
 * <pre>{@code
 * Directory dir = SearchableSnapshotDirectoryFactory.newDirectory(path);
 * try (IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT)) {
 *     // write data ...
 *     CodecUtil.writeFooter(out); // a valid footer is required
 * }
 * // after the IndexOutput is closed, the snapshot is materialized and ready for reads
 * try (IndexInput in = dir.openInput("vectors", IOContext.DEFAULT)) {
 *     // read data ...
 * }
 * dir.close(); // tears down all SNAP infrastructure
 * }</pre>
 */
public class SearchableSnapshotDirectoryFactory {

    /**
     * Returns a {@link Directory} that buffers {@link IndexOutput} writes, then materializes a
     * {@link SearchableSnapshotDirectory} when the output is closed.
     */
    public static Directory newDirectory(Path path) {
        return new WriteOnceSnapshotDirectory(path);
    }

    // -- WriteOnceSnapshotDirectory --

    static class WriteOnceSnapshotDirectory extends Directory {
        private final Path path;
        private String fileName;
        private byte[] data;
        private SearchableSnapshotDirectory delegate;

        // infrastructure — created lazily when the snapshot is materialized
        private ThreadPool threadPool;
        private CacheService cacheService;
        private ClusterService clusterService;
        private SharedBlobCacheService<CacheKey> sharedBlobCacheService;
        private NodeEnvironment nodeEnvironment;

        WriteOnceSnapshotDirectory(Path path) {
            this.path = path;
        }

        @Override
        public String[] listAll() throws IOException {
            ensureDelegate();
            return delegate.listAll();
        }

        @Override
        public void deleteFile(String name) {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public long fileLength(String name) throws IOException {
            ensureDelegate();
            return delegate.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) {
            this.fileName = name;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            return new SnapIndexOutput(name, buffer);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public void sync(Collection<String> names) {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public void syncMetaData() {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public void rename(String source, String dest) {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            ensureDelegate();
            return delegate.openInput(name, context);
        }

        @Override
        public Lock obtainLock(String name) {
            throw new UnsupportedOperationException("write-once, read-many directory");
        }

        @Override
        public Set<String> getPendingDeletions() {
            return Set.of();
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeWhileHandlingException(sharedBlobCacheService, clusterService, cacheService, nodeEnvironment);
            if (delegate != null) {
                delegate.close();
            }
            if (threadPool != null) {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }

        private void ensureDelegate() {
            if (delegate == null) {
                throw new IllegalStateException("snapshot not yet materialized — close the IndexOutput first");
            }
        }

        private void materializeSnapshot() {
            try {
                materializeSnapshotImpl();
            } catch (IOException e) {
                throw new RuntimeException("Failed to materialize searchable snapshot", e);
            }
        }

        private void materializeSnapshotImpl() throws IOException {
            final String blobName = "blob-1";
            final BlobContainer blobContainer = singleBlobContainer(blobName, data);

            final String checksum = "0";
            final StoreFileMetadata metadata = new StoreFileMetadata(
                fileName,
                data.length,
                checksum,
                IndexVersion.current().luceneVersion().toString()
            );
            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(
                "snapshotId",
                List.of(new BlobStoreIndexShardSnapshot.FileInfo(blobName, metadata, ByteSizeValue.ofBytes(data.length + 1))),
                0L,
                0L,
                0,
                0L
            );
            SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            IndexId indexId = new IndexId("_name", "_uuid");
            ShardId shardId = new ShardId("_name", "_uuid", 0);
            Path topDir = path.resolve(shardId.getIndex().getUUID());
            Path shardDir = topDir.resolve(Integer.toString(shardId.getId()));
            ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
            Path cacheDir = Files.createDirectories(CacheService.resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
            threadPool = new SimpleThreadPool("tp", SearchableSnapshots.executorBuilders(Settings.EMPTY));
            nodeEnvironment = newNodeEnvironment(Settings.EMPTY, path, data.length);
            clusterService = createClusterService(threadPool, clusterSettings());

            Settings.Builder cacheSettings = Settings.builder();
            cacheService = new CacheService(cacheSettings.build(), clusterService, threadPool, new PersistentCache(nodeEnvironment));
            cacheService.start();
            sharedBlobCacheService = defaultFrozenCacheService(threadPool, nodeEnvironment, path, data.length);

            delegate = new SearchableSnapshotDirectory(
                () -> blobContainer,
                () -> snapshot,
                new NoopBlobStoreCacheService(threadPool),
                "_repo",
                snapshotId,
                indexId,
                shardId,
                Settings.EMPTY,
                () -> 0L,
                cacheService,
                cacheDir,
                shardPath,
                threadPool,
                sharedBlobCacheService
            );

            // load the snapshot so it's ready for reads
            SearchableSnapshotRecoveryState recoveryState = createRecoveryState(false);
            final PlainActionFuture<Void> f = new PlainActionFuture<>();
            delegate.loadSnapshot(recoveryState, () -> false, f);
            try {
                f.get();
            } catch (Exception e) {
                throw new IOException("Failed to load snapshot", e);
            }
        }

        // -- SnapIndexOutput: hooks close() to materialize the snapshot --

        private class SnapIndexOutput extends OutputStreamIndexOutput {
            private final ByteArrayOutputStream buffer;

            SnapIndexOutput(String name, ByteArrayOutputStream buffer) {
                super(name, name, buffer, 8192);
                this.buffer = buffer;
            }

            @Override
            public void close() throws IOException {
                super.close();
                data = buffer.toByteArray();
                checkFooter(data);
                materializeSnapshot();
            }

            // Footers can be handled specially - cached, so ensure that the footer
            // is present and valid.
            static void checkFooter(byte[] data) throws IOException {
                try (var in = new BufferedChecksumIndexInput(new ByteArrayIndexInput("verify footer", data))) {
                    in.seek(in.length() - CodecUtil.footerLength());
                    CodecUtil.checkFooter(in);
                }
            }
        }
    }

    // -- cluster service creation (inlined from ClusterServiceUtils) --

    private static ClusterService createClusterService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            "node",
            "node",
            "host",
            "host",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Map.of(),
            DiscoveryNode.getRolesFromSettings(Settings.EMPTY),
            null
        );

        Settings settings = Settings.builder().put("node.name", "test").put("cluster.name", "ClusterServiceTests").build();
        TaskManager taskManager = new TaskManager(settings, threadPool, Collections.emptySet(), Tracer.NOOP, discoveryNode.getId());
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool, taskManager);
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {}
        });
        ClusterState initialState = ClusterState.builder(new ClusterName("SearchableSnapshotDirectoryFactory"))
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).masterNodeId(discoveryNode.getId()))
            .putCompatibilityVersions(discoveryNode.getId(), new CompatibilityVersions(TransportVersion.current(), Map.of()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialState);
        clusterService.getMasterService().setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            clusterStatePublicationEvent.setPublicationContextConstructionElapsedMillis(0L);
            clusterStatePublicationEvent.setPublicationCommitElapsedMillis(0L);
            clusterStatePublicationEvent.setPublicationCompletionElapsedMillis(0L);
            clusterStatePublicationEvent.setMasterApplyElapsedMillis(0L);
            clusterService.getClusterApplierService()
                .onNewClusterState(
                    "mock_publish_to_self[" + clusterStatePublicationEvent.getSummary() + "]",
                    clusterStatePublicationEvent::getNewState,
                    publishListener
                );
        });
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    // -- cluster and env settings --

    private static ClusterSettings clusterSettings() {
        return new ClusterSettings(
            Settings.EMPTY,
            Sets.union(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Set.of(
                    CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING,
                    CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING,
                    CacheService.SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING
                )
            )
        );
    }

    private static Settings buildEnvSettings(Settings settings, Path path, long dataLength) {
        long sizeInBytes = roundUpTo16MB(dataLength);
        return Settings.builder()
            .put(settings)
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), 0L)
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .putList(Environment.PATH_DATA_SETTING.getKey(), path.toAbsolutePath().toString())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(sizeInBytes).getStringRep())
            .build();
    }

    public static long roundUpTo16MB(long value) {
        long block = 16L * 1024 * 1024; // 16 MB
        return ((value + block - 1) / block) * block;
    }

    private static NodeEnvironment newNodeEnvironment(Settings settings, Path path, long dataLength) throws IOException {
        Settings build = buildEnvSettings(settings, path, dataLength);
        Settings envBuild = buildEnvSettings(settings, path, dataLength);
        return new NodeEnvironment(build, new Environment(envBuild, null));
    }

    private static SharedBlobCacheService<CacheKey> defaultFrozenCacheService(
        ThreadPool threadPool,
        NodeEnvironment nodeEnvironment,
        Path path,
        long dataLength
    ) {
        return new SharedBlobCacheService<>(
            nodeEnvironment,
            buildEnvSettings(Settings.EMPTY, path, dataLength),
            threadPool,
            threadPool.executor(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME),
            BlobCacheMetrics.NOOP
        );
    }

    // -- blob container --

    private static BlobContainer singleBlobContainer(final String blobName, final byte[] blobContent) {
        return new MostlyUnimplementedFakeBlobContainer() {
            @Override
            public InputStream readBlob(OperationPurpose purpose, String name, long position, long length) throws IOException {
                if (blobName.equals(name) == false) {
                    throw new FileNotFoundException("Blob not found: " + name);
                }
                return Streams.limitStream(
                    new ByteArrayInputStream(blobContent, toIntBytes(position), blobContent.length - toIntBytes(position)),
                    length
                );
            }
        };
    }

    // -- recovery state (inlined from TestShardRouting + DiscoveryNodeUtils) --

    private static SearchableSnapshotRecoveryState createRecoveryState(boolean finalizedDone) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId("a", "b", 0),
            true,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("repo", new SnapshotId("z", UUIDs.randomBase64UUID())),
                IndexVersion.current(),
                new IndexId("some_index", UUIDs.randomBase64UUID())
            ),
            new UnassignedInfo(
                UnassignedInfo.Reason.INDEX_CREATED,
                "message",
                null,
                0,
                1,
                1,
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Set.of(),
                ""
            ),
            ShardRouting.Role.DEFAULT
        ).initialize("node1", "existingAllocationId", ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

        DiscoveryNode targetNode = new DiscoveryNode(
            "local",
            "local",
            "local",
            "host",
            "host",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Map.of(),
            DiscoveryNode.getRolesFromSettings(Settings.EMPTY),
            null
        );
        SearchableSnapshotRecoveryState recoveryState = new SearchableSnapshotRecoveryState(shardRouting, targetNode, null);

        recoveryState.setStage(RecoveryState.Stage.INIT)
            .setStage(RecoveryState.Stage.INDEX)
            .setStage(RecoveryState.Stage.VERIFY_INDEX)
            .setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        if (finalizedDone) {
            recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);
        }
        return recoveryState;
    }

    // -- inner classes --

    static class MostlyUnimplementedFakeBlobContainer implements BlobContainer {

        @Override
        public long readBlobPreferredLength() {
            return Long.MAX_VALUE;
        }

        @Override
        public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) {
            throw unsupportedException();
        }

        @Override
        public BlobPath path() {
            throw unsupportedException();
        }

        @Override
        public boolean blobExists(OperationPurpose purpose, String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            throw unsupportedException();
        }

        @Override
        public void writeBlob(
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) {
            throw unsupportedException();
        }

        @Override
        public void writeMetadataBlob(
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) {
            throw unsupportedException();
        }

        @Override
        public void writeBlobAtomic(
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            throw unsupportedException();
        }

        @Override
        public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
            throw unsupportedException();
        }

        @Override
        public DeleteResult delete(OperationPurpose purpose) {
            throw unsupportedException();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobContainer> children(OperationPurpose purpose) {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) {
            throw unsupportedException();
        }

        @Override
        public void compareAndExchangeRegister(
            OperationPurpose purpose,
            String key,
            BytesReference expected,
            BytesReference updated,
            ActionListener<OptionalBytesReference> listener
        ) {
            listener.onFailure(unsupportedException());
        }

        @Override
        public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
            listener.onFailure(unsupportedException());
        }

        private UnsupportedOperationException unsupportedException() {
            assert false : "this operation is not supported and should have not be called";
            return new UnsupportedOperationException("This operation is not supported");
        }
    }

    static class NoopBlobStoreCacheService extends BlobStoreCacheService {

        NoopBlobStoreCacheService(ThreadPool threadPool) {
            super(new NoOpInternalClient(threadPool), SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX);
        }

        @Override
        protected void innerGet(GetRequest request, ActionListener<GetResponse> listener) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }

        @Override
        protected void innerPut(IndexRequest request, ActionListener<DocWriteResponse> listener) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }

        @Override
        public ByteRange computeBlobCacheByteRange(ShardId shardId, String fileName, long fileLength, ByteSizeValue maxMetadataLength) {
            return ByteRange.EMPTY;
        }
    }

    /**
     * A minimal client that does nothing — replacement for test:framework's NoOpClient.
     */
    private static class NoOpInternalClient extends AbstractClient {
        NoOpInternalClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool, DefaultProjectResolver.INSTANCE);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse(null);
        }
    }

    static class SimpleThreadPool extends ThreadPool implements Releasable {
        SimpleThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
            this(name, Settings.EMPTY, customBuilders);
        }

        SimpleThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
            super(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders(),
                customBuilders
            );
        }

        @Override
        public void close() {
            ThreadPool.terminate(this, 10, TimeUnit.SECONDS);
        }
    }

    private static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }
}
