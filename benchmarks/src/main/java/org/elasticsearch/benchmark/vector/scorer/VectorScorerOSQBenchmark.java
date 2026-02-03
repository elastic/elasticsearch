/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.simdvec.BufferedIndexInputWrapper;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorScorerOSQBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
        LogConfigurator.loadLog4jPlugins();
    }

    public enum DirectoryType {
        NIO, // this might be good enough to approximate Serverless vector ops perf
        MMAP,
        // could I add blob-cache here somehow?
        SNAP
    }

    public enum VectorImplementation {
        SCALAR,
        VECTORIZED
    }

    @Param({ "384", "768", "1024" })
    public int dims;

    // @Param({ "1", "2", "4" })
    @Param({ "1" })
    public int bits;

    int bulkSize = ESNextOSQVectorsScorer.BULK_SIZE;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    @Param
    public VectorSimilarityFunction similarityFunction;

    public int numVectors = ESNextOSQVectorsScorer.BULK_SIZE * 10;
    int numQueries = 10;

    int length;

    byte[][] binaryVectors;
    byte[][] binaryQueries;
    OptimizedScalarQuantizer.QuantizationResult result;
    float centroidDp;

    byte[] scratch;
    ESNextOSQVectorsScorer scorer;

    Path tempDir;
    Directory directory;
    IndexInput input;

    float[] scratchScores;
    float[] corrections;

    static final int INPUT_BUFFER_SIZE = 1 << 16; // 64k

    @Setup
    public void setup() throws IOException {
        setup(new Random(123));
    }

    void setup(Random random) throws IOException {
        this.length = switch (bits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dims);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dims);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dims);
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };

        binaryVectors = new byte[numVectors][length];
        for (byte[] binaryVector : binaryVectors) {
            random.nextBytes(binaryVector);
        }

        directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(createTempDirectory("vectorDataNFIOS"));
            case SNAP -> newSearchableSnapshotDirectory(createTempDirectory("vectorDataSNAP"), dataAsBytes(random));
        };

        if (directoryType != DirectoryType.SNAP) {
            try (IndexOutput output = directory.createOutput("vectors", IOContext.DEFAULT)) {
                byte[] correctionBytes = new byte[16 * bulkSize];
                for (int i = 0; i < numVectors; i += bulkSize) {
                    for (int j = 0; j < bulkSize; j++) {
                        output.writeBytes(binaryVectors[i + j], 0, binaryVectors[i + j].length);
                    }
                    random.nextBytes(correctionBytes);
                    output.writeBytes(correctionBytes, 0, correctionBytes.length);
                }
            }
        } else {
            RecoveryState recoveryState = createRecoveryState(false);
            final PlainActionFuture<Void> f = new PlainActionFuture<>();
            final boolean loaded = ((SearchableSnapshotDirectory) directory).loadSnapshot(recoveryState, () -> false, f);
            try {
                f.get();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        input = directory.openInput("vectors", IOContext.DEFAULT);
        input = BufferedIndexInputWrapper.wrap("vectors wrapper", input, INPUT_BUFFER_SIZE);
        int binaryQueryLength = switch (bits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getQueryPackedLength(dims);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getQueryPackedLength(dims);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getQueryPackedLength(dims);
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };

        binaryQueries = new byte[numVectors][binaryQueryLength];
        for (byte[] binaryQuery : binaryQueries) {
            random.nextBytes(binaryQuery);
        }
        result = new OptimizedScalarQuantizer.QuantizationResult(
            random.nextFloat(),
            random.nextFloat(),
            random.nextFloat(),
            Short.toUnsignedInt((short) random.nextInt())
        );
        centroidDp = random.nextFloat();

        scratch = new byte[length];
        final int docBits;
        final int queryBits = switch (bits) {
            case 1 -> {
                docBits = 1;
                yield 4;
            }
            case 2 -> {
                docBits = 2;
                yield 4;
            }
            case 4 -> {
                docBits = 4;
                yield 4;
            }
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };
        scorer = switch (implementation) {
            case SCALAR -> new ESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length, bulkSize);
        };
        scratchScores = new float[bulkSize];
        corrections = new float[3];
    }

    Path createTempDirectory(String name) throws IOException {
        tempDir = Files.createTempDirectory(name);
        return tempDir;
    }

    byte[] dataAsBytes(Random random) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] correctionBytes = new byte[14 * bulkSize];
        for (int i = 0; i < numVectors; i += bulkSize) {
            for (int j = 0; j < bulkSize; j++) {
                baos.write(binaryVectors[i + j], 0, binaryVectors[i + j].length);
            }
            random.nextBytes(correctionBytes);
            baos.writeBytes(correctionBytes);
        }
        return baos.toByteArray();
    }

    @TearDown
    public void teardown() throws Exception {
        IOUtils.close(directory, input, cacheService, nodeEnvironment);
        IOUtils.rm(tempDir);
        threadPool.shutdownNow();
    }

    @Benchmark
    public float[] score() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i++) {
                float qDist = scorer.quantizeScore(binaryQueries[j]);
                input.readFloats(corrections, 0, corrections.length);
                int addition = Short.toUnsignedInt(input.readShort());
                float score = scorer.score(
                    result.lowerInterval(),
                    result.upperInterval(),
                    result.quantizedComponentSum(),
                    result.additionalCorrection(),
                    VectorSimilarityFunction.EUCLIDEAN,
                    centroidDp,
                    corrections[0],
                    corrections[1],
                    addition,
                    corrections[2],
                    qDist
                );
                results[j * numVectors + i] = score;
            }
        }
        return results;
    }

    @Benchmark
    public float[] bulkScore() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i += scratchScores.length) {
                scorer.scoreBulk(
                    binaryQueries[j],
                    result.lowerInterval(),
                    result.upperInterval(),
                    result.quantizedComponentSum(),
                    result.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scratchScores
                );
                System.arraycopy(scratchScores, 0, results, j * numVectors + i, scratchScores.length);
            }
        }
        return results;
    }

    // -- below here are all things related to searchable snapshots

    ThreadPool threadPool = new TestThreadPool("tp", SearchableSnapshots.executorBuilders(Settings.EMPTY));
    CacheService cacheService;
    NodeEnvironment nodeEnvironment;

    static ClusterSettings clusterSettings() {
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

    static Settings buildEnvSettings(Settings settings, Path path) {
        return Settings.builder()
            .put(settings)
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), 0L)
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .putList(Environment.PATH_DATA_SETTING.getKey(), path.toAbsolutePath().toString())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1).getStringRep())
            .build();
    }

    Directory newSearchableSnapshotDirectory(Path path, byte[] data) throws IOException {
        final String blobName = "blob-1";
        final byte[] input = data;
        final BlobContainer blobContainer = singleBlobContainer(blobName, input);

        final String checksum = "0";
        final StoreFileMetadata metadata = new StoreFileMetadata(
            "vectors",
            input.length,
            checksum,
            IndexVersion.current().luceneVersion().toString()
        );
        final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(
            "snapshotId",
            List.of(new BlobStoreIndexShardSnapshot.FileInfo(blobName, metadata, ByteSizeValue.ofBytes(input.length + 1))),
            0L,
            0L,
            0,
            0L
        );
        SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
        IndexId indexId = new IndexId("_name", "_uuid");
        ShardId shardId = new ShardId("_name", "_uuid", 0);
        Path topDir = path.resolve(shardId.getIndex().getUUID());
        Path shardDir = topDir.resolve(Path.of(Integer.toString(shardId.getId())));
        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
        Path cacheDir = Files.createDirectories(CacheService.resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
        nodeEnvironment = newNodeEnvironment(Settings.EMPTY, path);
        var clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings());

        Settings.Builder cacheSettings = Settings.builder();
        cacheService = new CacheService(cacheSettings.build(), clusterService, threadPool, new PersistentCache(nodeEnvironment));
        cacheService.start();
        final SharedBlobCacheService<CacheKey> sharedBlobCacheService = defaultFrozenCacheService(threadPool, nodeEnvironment, path);

        SearchableSnapshotDirectory searchableSnapshotDirectory = new SearchableSnapshotDirectory(
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
        return searchableSnapshotDirectory;
    }

    static NodeEnvironment newNodeEnvironment(Settings settings, Path path) throws IOException {
        Settings build = buildEnvSettings(settings, path);
        Settings envBuild = buildEnvSettings(settings, path);
        return new NodeEnvironment(build, new Environment(envBuild, null));
    }

    static SharedBlobCacheService<CacheKey> defaultFrozenCacheService(ThreadPool threadPool, NodeEnvironment nodeEnvironment, Path path) {
        return new SharedBlobCacheService<>(
            nodeEnvironment,
            buildEnvSettings(Settings.EMPTY, path),
            threadPool,
            threadPool.executor(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME),
            BlobCacheMetrics.NOOP
        );
    }

    static BlobContainer singleBlobContainer(final String blobName, final byte[] blobContent) {
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

    static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }

    static class NoopBlobStoreCacheService extends BlobStoreCacheService {

        NoopBlobStoreCacheService(ThreadPool threadPool) {
            super(new NoOpClient(threadPool), SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX);
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

    static SearchableSnapshotRecoveryState createRecoveryState(boolean finalizedDone) {
        ShardRouting shardRouting = TestShardRouting.shardRoutingBuilder(
            new ShardId("a", "b", 0),
            "node1",
            true,
            ShardRoutingState.INITIALIZING
        )
            .withRecoverySource(
                new RecoverySource.SnapshotRecoverySource(
                    UUIDs.randomBase64UUID(),
                    new Snapshot("repo", new SnapshotId("z", UUIDs.randomBase64UUID())),
                    IndexVersion.current(),
                    new IndexId("some_index", UUIDs.randomBase64UUID())
                )
            )
            .withUnassignedInfo(anyUnassignedInfo())
            .withAllocationId(anyAllocationID())
            .build();
        DiscoveryNode targetNode = DiscoveryNodeUtils.create("local");
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

    static UnassignedInfo anyUnassignedInfo() {
        return new UnassignedInfo(
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
        );
    }

    static AllocationId anyAllocationID() {
        return AllocationId.newInitializing("existingAllocationId");
    }

    static class TestThreadPool extends ThreadPool implements Releasable {
        TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
            this(name, Settings.EMPTY, customBuilders);
        }

        TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
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
}
