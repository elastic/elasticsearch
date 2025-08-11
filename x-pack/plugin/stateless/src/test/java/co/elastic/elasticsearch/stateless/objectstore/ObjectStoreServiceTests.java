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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StaleCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.ObjectStoreType;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;
import co.elastic.elasticsearch.stateless.utils.TransferableCloseables;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.BUCKET_SETTING;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.ObjectStoreType.AZURE;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.ObjectStoreType.FS;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.ObjectStoreType.GCS;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.ObjectStoreType.S3;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
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
        String basePath = randomBoolean() ? randomAlphaOfLength(5) : null;

        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), FS.name());
        builder.put(ObjectStoreService.BUCKET_SETTING.getKey(), bucket);
        if (basePath != null) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), basePath);
        }
        // no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.createRepositorySettings(bucket, randomAlphaOfLength(5), basePath);
        assertThat(settings.keySet().size(), equalTo(1));
        assertThat(settings.get("location"), equalTo(basePath != null ? PathUtils.get(bucket, basePath).toString() : bucket));
    }

    public void testObjectStoreSettings() {
        validateObjectStoreSettings(S3, "bucket");
        validateObjectStoreSettings(GCS, "bucket");
        validateObjectStoreSettings(AZURE, "container");
    }

    private void validateObjectStoreSettings(ObjectStoreType type, String bucketName) {
        String bucket = randomAlphaOfLength(5);
        String client = randomAlphaOfLength(5);
        String basePath = randomBoolean() ? randomAlphaOfLength(5) : null;

        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE_SETTING.getKey(), type.name());
        builder.put(ObjectStoreService.BUCKET_SETTING.getKey(), bucket);
        builder.put(ObjectStoreService.CLIENT_SETTING.getKey(), client);
        if (basePath != null) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), basePath);
        }
        // check no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE_SETTING.get(builder.build());
        Settings settings = objectStoreType.createRepositorySettings(bucket, client, basePath);
        assertThat(
            settings.keySet().size(),
            equalTo(2 + (basePath == null ? 0 : 1) + (objectStoreType == S3 ? 1 /* add_purpose_custom_query_parameter */ : 0))
        );
        assertThat(settings.get(bucketName), equalTo(bucket));
        assertThat(settings.get("client"), equalTo(client));
        assertThat(settings.get("base_path"), equalTo(basePath));
    }

    public void testStartingShardRetrievesSegmentsFromOneCommit() throws IOException {
        final var mergesEnabled = randomBoolean();
        final var indexWriterConfig = mergesEnabled
            ? new IndexWriterConfig(new KeywordAnalyzer())
            : Lucene.indexWriterConfigWithNoMerging(new KeywordAnalyzer());

        final var permittedFiles = new HashSet<String>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
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
                    public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                        assert StatelessCompoundCommit.startsWithBlobPrefix(blobName) || permittedFiles.contains(blobName)
                            : blobName + " in " + permittedFiles;
                        return super.readBlob(purpose, blobName);
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }

            @Override
            protected Settings nodeSettings() {
                // the future wait below assumes every commit is released immediately, not true when delayed.
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                    .build();
            }
        }) {
            var commitCount = between(0, 5);

            try (
                var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig);
                var closeables = new TransferableCloseables()
            ) {

                Set<String> previousCommitFiles = Collections.emptySet();
                for (int commit = 0; commit < commitCount; commit++) {
                    indexWriter.addDocument(List.of());
                    indexWriter.forceMerge(1);
                    indexWriter.commit();
                    final var indexReader = closeables.add(DirectoryReader.open(indexWriter));
                    final var indexCommit = indexReader.getIndexCommit();
                    final var commitFiles = testHarness.indexingStore.getMetadata(indexCommit).fileMetadataMap().keySet();
                    ;
                    final var additionalFiles = Sets.difference(commitFiles, previousCommitFiles);
                    previousCommitFiles = commitFiles;
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

                    final var commitCloseLatch = new CountDownLatch(1);
                    testHarness.commitService.onCommitCreation(
                        new StatelessCommitRef(
                            testHarness.shardId,
                            new Engine.IndexCommitRef(indexCommit, commitCloseLatch::countDown),
                            additionalFiles,
                            1,
                            0,
                            0
                        )
                    );
                    safeAwait(commitCloseLatch);
                }
            }

            assertEquals(
                commitCount,
                testHarness.objectStoreService.getProjectBlobContainer(testHarness.shardId, 1)
                    .listBlobs(randomFrom(OperationPurpose.values()))
                    .keySet()
                    .stream()
                    .filter(StatelessCompoundCommit::startsWithBlobPrefix)
                    .count()
            );

            final var dir = SearchDirectory.unwrapDirectory(testHarness.searchStore.directory());
            BatchedCompoundCommit commit = ObjectStoreService.readSearchShardState(
                testHarness.objectStoreService.getProjectBlobContainer(testHarness.shardId),
                1
            );
            if (commit != null) {
                dir.updateLatestUploadedBcc(commit.primaryTermAndGeneration());
                dir.updateLatestCommitInfo(commit.primaryTermAndGeneration(), testHarness.clusterService.localNode().getId());
                dir.updateCommit(commit.lastCompoundCommit());
            }

            if (commitCount > 0) {
                assertEquals(permittedFiles, Set.of(dir.listAll()));
            }

            try (var indexReader = DirectoryReader.open(testHarness.searchStore.directory())) {
                assertEquals(commitCount, indexReader.numDocs());
            }
        }
    }

    public void testReadNewestCommit() throws Exception {
        final Map<String, BlobLocation> uploadedBlobLocations = ConcurrentCollections.newConcurrentMap();
        final var notifiedCompoundCommits = new ConcurrentLinkedQueue<StatelessCompoundCommit>();
        final long primaryTerm = randomLongBetween(1, 42);

        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                // Intercept the client call in order to
                // (1) We can be sure that a CC has been uploaded to the object store
                // (2) Capture the uploaded StatelessCompoundCommit to build BlobLocations that can be used by BCCs
                return new NoOpNodeClient(threadPool) {
                    @SuppressWarnings("unchecked")
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        assert action == TransportNewCommitNotificationAction.TYPE;
                        notifiedCompoundCommits.add(((NewCommitNotificationRequest) request).getCompoundCommit());
                        ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
                    }
                };
            }

            @Override
            protected Settings nodeSettings() {
                // expect 1 commit per bcc below
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                    .build();
            }
        }) {
            final BlobContainer shardBlobContainer = testHarness.objectStoreService.getProjectBlobContainer(
                testHarness.shardId,
                primaryTerm
            );
            final AtomicReference<StatelessCompoundCommit> expectedNewestCompoundCommit = new AtomicReference<>();

            // The node may already have existing CCs
            final int ccCount = between(0, 4);
            for (var indexCommit : testHarness.generateIndexCommits(ccCount)) {
                testHarness.commitService.onCommitCreation(indexCommit);
                assertBusy(() -> {
                    Optional<StatelessCompoundCommit> optionalCompoundCommit = notifiedCompoundCommits.stream()
                        .filter(c -> c.primaryTerm() == indexCommit.getPrimaryTerm() && c.generation() == indexCommit.getGeneration())
                        .findFirst();
                    assertTrue(optionalCompoundCommit.isPresent());
                    var compoundCommit = optionalCompoundCommit.get();
                    // Update the blobLocations so that BCCs can use them later
                    uploadedBlobLocations.putAll(compoundCommit.commitFiles());
                    expectedNewestCompoundCommit.set(compoundCommit);
                });
            }

            // Should find the latest compound commit from a list of pure CCs
            if (ccCount > 0) {
                var waitForUploadLatch = new CountDownLatch(1);
                testHarness.commitService.addListenerForUploadedGeneration(
                    testHarness.shardId,
                    expectedNewestCompoundCommit.get().generation(),
                    ActionListener.releasing(waitForUploadLatch::countDown)
                );
                safeAwait(waitForUploadLatch);
                assertThat(
                    ObjectStoreService.readNewestBcc(shardBlobContainer, shardBlobContainer.listBlobs(OperationPurpose.INDICES)),
                    equalTo(
                        new BatchedCompoundCommit(
                            expectedNewestCompoundCommit.get().primaryTermAndGeneration(),
                            List.of(expectedNewestCompoundCommit.get())
                        )
                    )
                );
            }

            // Add BCCs after the existing CCs
            final int bccCount = between(1, 4);
            final AtomicReference<BatchedCompoundCommit> expectedNewestBatchedCompoundCommit = new AtomicReference<>();

            for (int i = 0; i < bccCount; i++) {
                final int ccPerBcc = between(1, 4);
                var indexCommits = testHarness.generateIndexCommits(ccPerBcc);
                long firstCommitGeneration = indexCommits.get(0).getGeneration();
                try (
                    var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                        testHarness.shardId,
                        "node-id",
                        primaryTerm,
                        firstCommitGeneration,
                        uploadedBlobLocations::get,
                        ESTestCase::randomNonNegativeLong,
                        testHarness.sharedCacheService.getRegionSize(),
                        randomIntBetween(0, testHarness.sharedCacheService.getRegionSize())
                    )
                ) {
                    for (StatelessCommitRef statelessCommitRef : indexCommits) {
                        assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean()));
                    }
                    virtualBatchedCompoundCommit.freeze();
                    try (var vbccInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                        shardBlobContainer.writeBlobAtomic(
                            OperationPurpose.INDICES,
                            virtualBatchedCompoundCommit.getBlobName(),
                            vbccInputStream,
                            virtualBatchedCompoundCommit.getTotalSizeInBytes(),
                            true
                        );
                    }
                    final BatchedCompoundCommit batchedCompoundCommit = virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit();
                    for (var compoundCommit : batchedCompoundCommit.compoundCommits()) {
                        uploadedBlobLocations.putAll(compoundCommit.commitFiles());
                    }
                    expectedNewestBatchedCompoundCommit.set(batchedCompoundCommit);
                }
            }

            // Should find the newest commit in a list of CCs and BCCs
            assertThat(
                ObjectStoreService.readNewestBcc(shardBlobContainer, shardBlobContainer.listBlobs(OperationPurpose.INDICES)),
                equalTo(expectedNewestBatchedCompoundCommit.get())
            );
        }
    }

    public void testReadLatestBccPopulatesCache() throws Exception {
        final Map<String, BlobLocation> uploadedBlobs = ConcurrentCollections.newConcurrentMap();
        var primaryTerm = randomLongBetween(1, 42);
        var useReplicatedRanges = randomBoolean();

        long latestBccLength = 0L;
        var cacheSize = ByteSizeValue.ofMb(1L);
        var regionSize = ByteSizeValue.ofBytes((long) randomIntBetween(1, 3) * SharedBytes.PAGE_SIZE);
        try (
            var testHarness = new FakeStatelessNode(
                TestEnvironment::newEnvironment,
                settings -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings)),
                xContentRegistry(),
                primaryTerm
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                        .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize)
                        .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), regionSize)
                        .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), useReplicatedRanges)
                        .build();
                }
            }
        ) {
            BatchedCompoundCommit latestBcc = null;
            var nbBlobs = randomIntBetween(0, 10);
            for (int i = 0; i < nbBlobs; i++) {
                int nbCommits = randomIntBetween(1, 10);
                // generate commits larger than the region size to ensure that the header and the padding are located in different regions
                var indexCommits = testHarness.generateIndexCommitsWithMinSegmentSize(nbCommits, regionSize.getBytes() + 1L);
                try (
                    var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                        testHarness.shardId,
                        "node-id",
                        primaryTerm,
                        indexCommits.getFirst().getGeneration(),
                        uploadedBlobs::get,
                        ESTestCase::randomNonNegativeLong,
                        testHarness.sharedCacheService.getRegionSize(),
                        randomIntBetween(0, testHarness.sharedCacheService.getRegionSize())
                    )
                ) {
                    for (var indexCommit : indexCommits) {
                        assertTrue(virtualBatchedCompoundCommit.appendCommit(indexCommit, useReplicatedRanges));
                        assertThat(getCommitSize(indexCommit), greaterThan(regionSize.getBytes()));
                    }
                    virtualBatchedCompoundCommit.freeze();

                    try (var stream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                        var shardContainer = testHarness.objectStoreService.getProjectBlobContainer(testHarness.shardId, primaryTerm);
                        shardContainer.writeBlobAtomic(
                            OperationPurpose.INDICES,
                            virtualBatchedCompoundCommit.getBlobName(),
                            stream,
                            virtualBatchedCompoundCommit.getTotalSizeInBytes(),
                            true
                        );
                    }
                    latestBcc = virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit();
                    latestBcc.compoundCommits().forEach(compoundCommit -> uploadedBlobs.putAll(compoundCommit.commitFiles()));
                    latestBccLength = virtualBatchedCompoundCommit.getTotalSizeInBytes();
                }
            }

            var directory = IndexBlobStoreCacheDirectory.unwrapDirectory(testHarness.indexingDirectory);
            var blobs = ObjectStoreService.listBlobs(primaryTerm, directory.getBlobContainer(primaryTerm));
            assertThat(blobs.size(), equalTo(nbBlobs));
            assertThat(ObjectStoreService.readLatestBcc(directory, IOContext.DEFAULT, blobs), equalTo(latestBcc));

            long writeCount = computeCacheWriteCounts(latestBcc, latestBccLength, regionSize.getBytes());
            assertTrue(TestThreadPool.terminate(testHarness.threadPool, 10L, TimeUnit.SECONDS));

            var cacheStats = testHarness.sharedCacheService.getStats();
            assertThat(cacheStats.writeBytes(), equalTo(writeCount * regionSize.getBytes()));
            assertThat(cacheStats.writeCount(), equalTo(writeCount));
            assertThat(cacheStats.missCount(), equalTo(writeCount));
            assertThat(cacheStats.evictCount(), equalTo(0L));

            if (0L < latestBccLength) {
                assertThat(cacheStats.writeBytes(), lessThan(latestBccLength));
            }
        }
    }

    public void testProjectObjectStores() throws IOException {
        final long primaryTerm = randomLongBetween(1, 42);
        try (
            var testHarness = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.allProjects()
            )
        ) {
            final var objectStoreService = testHarness.objectStoreService;
            final String blobName = "test_blob";

            final BlobStoreRepository clusterObjectStore = objectStoreService.getClusterObjectStore();
            assertNull(clusterObjectStore.getProjectId());
            assertThat(
                objectStoreService.getProjectObjectStores(),
                equalTo(Map.of(ProjectId.DEFAULT, objectStoreService.getClusterObjectStore()))
            );
            assertThat(objectStoreService.getProjectObjectStore(ProjectId.DEFAULT), sameInstance(clusterObjectStore));

            clusterObjectStore.blobStore()
                .blobContainer(BlobPath.EMPTY)
                .writeBlob(randomFrom(OperationPurpose.values()), blobName, new BytesArray("cluster"), true);

            // create some projects
            final List<ProjectId> projectIds = randomList(1, 3, ESTestCase::randomUniqueProjectId);
            final Map<ProjectId, BlobStoreRepository> projectObjectStores = new HashMap<>();
            for (int i = 0; i < projectIds.size(); i++) {
                final ProjectId projectId = projectIds.get(i);
                assertProjectObjectStoreNotFound(objectStoreService, projectId);
                final var projectSettingsBuilder = Settings.builder()
                    .put("stateless.object_store.bucket", "bucket_" + projectId)
                    .put("stateless.object_store.base_path", "base_path");
                if (randomBoolean()) {
                    projectSettingsBuilder.put("stateless.object_store.type", "fs");
                }

                ClusterState state = testHarness.clusterService.state();
                ClusterServiceUtils.setState(
                    testHarness.clusterService,
                    ClusterState.builder(state)
                        .putCustom(
                            ProjectStateRegistry.TYPE,
                            ProjectStateRegistry.builder(state).putProjectSettings(projectId, projectSettingsBuilder.build()).build()
                        )
                        .putProjectMetadata(ProjectMetadata.builder(projectId))
                        .build()
                );
                // We should always have the default project object store (i.e., 1)
                // plus the number of object stores equal to the number of created projects (i.e., i + 1)
                assertThat(objectStoreService.getProjectObjectStores(), aMapWithSize(1 + i + 1));

                final BlobStoreRepository projectObjectStore = objectStoreService.getProjectObjectStore(projectId);
                assertNotNull(projectObjectStore);
                assertThat(projectObjectStore.getProjectId(), equalTo(projectId));
                assertThat(projectObjectStore.getMetadata().type(), equalTo("fs"));
                assertThat(
                    projectObjectStore.getMetadata().settings().get("location"),
                    equalTo(PathUtils.get("bucket_" + projectId, "base_path").toString())
                );
                assertThat(projectObjectStore.lifecycleState(), equalTo(Lifecycle.State.STARTED));

                // Write the blob of the same name should succeed since the base path is different
                projectObjectStore.blobStore()
                    .blobContainer(BlobPath.EMPTY)
                    .writeBlob(randomFrom(OperationPurpose.values()), blobName, new BytesArray(projectId.id()), true);

                projectObjectStores.put(projectId, projectObjectStore);
            }

            // Delete the projects
            for (int i = 0; i < projectIds.size(); i++) {
                final ProjectId projectId = projectIds.get(i);
                final ClusterState state = testHarness.clusterService.state();
                ClusterServiceUtils.setState(
                    testHarness.clusterService,
                    ClusterState.builder(state)
                        .metadata(Metadata.builder(state.metadata()).removeProject(projectId))
                        .routingTable(GlobalRoutingTable.builder(state.globalRoutingTable()).removeProject(projectId).build())
                        .build()
                );
                // We should always have the default project object store (i.e., 1)
                // plus the number of object stores equal to the number of existing projects (i.e., projectIds.size() - i - 1)
                assertThat(objectStoreService.getProjectObjectStores(), aMapWithSize(1 + projectIds.size() - i - 1));
                assertThat(objectStoreService.getProjectObjectStores(), not(hasKey(projectId)));
                assertProjectObjectStoreNotFound(objectStoreService, projectId);
                assertThat(projectObjectStores.get(projectId).lifecycleState(), equalTo(Lifecycle.State.CLOSED));
            }
        }
    }

    public void testProjectObjectStoreExceptions() throws IOException {
        final long primaryTerm = randomLongBetween(1, 42);
        final ProjectId projectId = randomUniqueProjectId();
        final var expectedException = randomBoolean() ? new RepositoryException(Stateless.NAME, "boom") : new RuntimeException("ugh");
        try (
            var testHarness = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.allProjects()
            ) {
                @Override
                protected RepositoriesService createRepositoryService(NamedXContentRegistry xContentRegistry) {
                    return new RepositoriesService(nodeSettings, clusterService, Map.of(FsRepository.TYPE, (projectId, metadata) -> {
                        if (metadata.settings().get("location").contains("bucket_" + projectId)) {
                            throw expectedException;
                        } else {
                            return createFsRepository(xContentRegistry, projectId, metadata);
                        }
                    }), Map.of(), threadPool, client, List.of(), SnapshotMetrics.NOOP);
                }
            }
        ) {
            final var objectStoreService = testHarness.objectStoreService;

            final var projectSettingsBuilder = Settings.builder()
                .put("stateless.object_store.bucket", "bucket_" + projectId)
                .put("stateless.object_store.base_path", "base_path");
            if (randomBoolean()) {
                projectSettingsBuilder.put("stateless.object_store.type", "fs");
            }

            ClusterState state = testHarness.clusterService.state();
            Settings settings = projectSettingsBuilder.build();
            ClusterServiceUtils.setState(
                testHarness.clusterService,
                ClusterState.builder(state)
                    .putCustom(
                        ProjectStateRegistry.TYPE,
                        ProjectStateRegistry.builder(state).putProjectSettings(projectId, settings).build()
                    )
                    .putProjectMetadata(ProjectMetadata.builder(projectId))
                    .build()
            );

            assertThat(objectStoreService.getProjectObjectStores().keySet(), contains(ProjectId.DEFAULT));
            assertNotNull(objectStoreService.getProjectObjectStores());
            assertThat(objectStoreService.getProjectObjectStoreExceptions().keySet(), contains(projectId));

            final var e = expectThrows(RepositoryException.class, () -> objectStoreService.getProjectObjectStore(projectId));
            assertThat(e.getCause(), is(expectedException));

            // Delete the project
            state = testHarness.clusterService.state();
            ClusterServiceUtils.setState(
                testHarness.clusterService,
                ClusterState.builder(state)
                    .metadata(Metadata.builder(state.metadata()).removeProject(projectId))
                    .routingTable(GlobalRoutingTable.builder(state.globalRoutingTable()).removeProject(projectId).build())
                    .build()
            );
            assertThat(objectStoreService.getProjectObjectStoreExceptions(), anEmptyMap());
            assertProjectObjectStoreNotFound(objectStoreService, projectId);
        }
    }

    public void testCopyShard() throws IOException {
        var primaryTerm = randomLongBetween(1, 42);

        // We build two nodes that share physical location of object store
        // to have a clean state when reading destination shard after a copy.
        var tempDir = createTempDir().toAbsolutePath();
        var node1 = new FakeStatelessNode(
            this::newEnvironment,
            this::newNodeEnvironment,
            xContentRegistry(),
            primaryTerm,
            TestProjectResolvers.allProjects()
        ) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(PATH_REPO_SETTING.getKey(), tempDir)
                    .put(BUCKET_SETTING.getKey(), "testCopyIndexingShardState")
                    // the wait on commitCloseLatch below assumes every commit is released immediately, not true when delayed.
                    .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                    .build();
            }
        };

        var node2 = new FakeStatelessNode(
            this::newEnvironment,
            this::newNodeEnvironment,
            xContentRegistry(),
            primaryTerm,
            TestProjectResolvers.allProjects()
        ) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(PATH_REPO_SETTING.getKey(), tempDir)
                    .put(BUCKET_SETTING.getKey(), "testCopyIndexingShardState")
                    .build();
            }
        };

        try (node1; node2) {
            ShardId sourceShardId = node1.shardId;
            ObjectStoreService objectStoreService = node1.objectStoreService;

            var commitCount = between(1, 5);
            var commitCloseLatch = new CountDownLatch(commitCount);

            var commits = node1.generateIndexCommits(commitCount, randomBoolean(), false, ignored -> commitCloseLatch.countDown());
            for (var indexCommit : commits) {
                node1.commitService.onCommitCreation(indexCommit);
            }
            safeAwait(commitCloseLatch);

            var destinationShardId = new ShardId(sourceShardId.getIndex(), node1.shardId.getId() + 1);

            objectStoreService.copyShard(sourceShardId, destinationShardId, primaryTerm);

            // Shards should now have the same data, let's read from destination shard.
            var dir = SearchDirectory.unwrapDirectory(node2.searchStore.directory());
            BatchedCompoundCommit commit = ObjectStoreService.readSearchShardState(
                node2.objectStoreService.getProjectBlobContainer(destinationShardId),
                primaryTerm
            );
            if (commit != null) {
                dir.updateLatestUploadedBcc(commit.primaryTermAndGeneration());
                dir.updateLatestCommitInfo(commit.primaryTermAndGeneration(), node2.clusterService.localNode().getId());
                dir.updateCommit(commit.lastCompoundCommit());
            }

            try (var indexReader = DirectoryReader.open(node2.searchStore.directory())) {
                assertEquals(commitCount, indexReader.numDocs());
                // Make sure shard data is actually usable with a deeper check.
                var indexSearcher = new IndexSearcher(indexReader);
                // See implementation of generateIndexCommits().
                assertEquals(commitCount, indexSearcher.search(new TermQuery(new Term("field0", "term")), 100).totalHits.value());
            }
        }
    }

    public void testDeletingShardFileSkipsNotFoundProject() throws Exception {
        final long primaryTerm = randomLongBetween(10, 42);

        final Set<String> deletedShardFiles = ConcurrentCollections.newConcurrentSet();

        try (
            var testHarness = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.allProjects()
            ) {
                @Override
                protected Settings nodeSettings() {
                    if (randomBoolean()) {
                        return super.nodeSettings();
                    } else {
                        return Settings.builder()
                            .put(super.nodeSettings())
                            .put(OBJECT_STORE_FILE_DELETION_DELAY.getKey(), TimeValue.timeValueMillis(between(10, 100)))
                            .build();
                    }
                }

                @Override
                public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                    return new FilterBlobContainer(innerContainer) {

                        @Override
                        protected BlobContainer wrapChild(BlobContainer child) {
                            return child;
                        }

                        @Override
                        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames)
                            throws IOException {
                            deletedShardFiles.addAll(Iterators.toList(blobNames));
                        }
                    };
                }
            }
        ) {
            final var objectStoreService = testHarness.objectStoreService;

            // Block shard file deletions
            final int maxDeletionThreads = testHarness.threadPool.info(Stateless.SHARD_WRITE_THREAD_POOL).getMax();
            final var startBarrier = new CyclicBarrier(maxDeletionThreads + 1);
            final var deletionBarrier = new CyclicBarrier(maxDeletionThreads + 1);
            for (int i = 0; i < maxDeletionThreads; i++) {
                testHarness.threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL).submit(() -> {
                    safeAwait(startBarrier);
                    safeAwait(deletionBarrier);
                });
            }
            safeAwait(startBarrier);

            // create a new project and an index
            final ProjectId projectId = randomUniqueProjectId();
            final var projectSettingsBuilder = Settings.builder()
                .put("stateless.object_store.bucket", "bucket_" + projectId)
                .put("stateless.object_store.base_path", "base_path");
            if (randomBoolean()) {
                projectSettingsBuilder.put("stateless.object_store.type", "fs");
            }
            final var indexMetadata = IndexMetadata.builder("index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                )
                .build();
            ClusterState state = testHarness.clusterService.state();
            Settings settings = projectSettingsBuilder.build();
            ClusterServiceUtils.setState(
                testHarness.clusterService,
                ClusterState.builder(state)
                    .putCustom(
                        ProjectStateRegistry.TYPE,
                        ProjectStateRegistry.builder(state).putProjectSettings(projectId, settings).build()
                    )
                    .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
                    .build()
            );
            final BlobStoreRepository projectObjectStore = objectStoreService.getProjectObjectStore(projectId);
            assertNotNull(projectObjectStore);

            // 1. Delete a shard file of an index that is not found in any project
            objectStoreService.asyncDeleteShardFile(
                new StaleCompoundCommit(
                    new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3)),
                    new PrimaryTermAndGeneration(primaryTerm, randomNonNegativeLong()),
                    primaryTerm
                )
            );
            // 2. Delete another shard file of an index that is found when deletion task is initially created but remove when it runs
            objectStoreService.asyncDeleteShardFile(
                new StaleCompoundCommit(
                    new ShardId(new Index(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID()), 0),
                    new PrimaryTermAndGeneration(primaryTerm, randomNonNegativeLong()),
                    primaryTerm
                )
            );
            // 3. Delete a normal shard file
            objectStoreService.asyncDeleteShardFile(
                new StaleCompoundCommit(
                    testHarness.shardId,
                    new PrimaryTermAndGeneration(primaryTerm, randomNonNegativeLong()),
                    primaryTerm
                )
            );

            // Delete the project
            state = testHarness.clusterService.state();
            ClusterServiceUtils.setState(
                testHarness.clusterService,
                ClusterState.builder(state)
                    .metadata(Metadata.builder(state.metadata()).removeProject(projectId))
                    .routingTable(GlobalRoutingTable.builder(state.globalRoutingTable()).removeProject(projectId).build())
                    .build()
            );

            // Unblock the shard file deletions
            safeAwait(deletionBarrier);

            // Shard file deletion is called for the remaining project and skipped for any project that is not found
            assertBusy(() -> {
                assertThat(deletedShardFiles.size(), equalTo(1));
                assertThat(deletedShardFiles.iterator().next(), startsWith("indices/" + testHarness.shardId.getIndex().getUUID()));
            });

        }
    }

    private void assertProjectObjectStoreNotFound(ObjectStoreService objectStoreService, ProjectId projectId) {
        final var e = expectThrows(RepositoryException.class, () -> objectStoreService.getProjectObjectStore(projectId));
        assertThat(e.getMessage(), equalTo("[" + Stateless.NAME + "] project [" + projectId + "] object store not found"));
    }

    private record CacheWriteCount(int writeCount, int regionStart, int regionEnd) {}

    /**
     * Computes the expected number of writes operations in cache that are required to fully read the bcc.
     */
    private static long computeCacheWriteCounts(BatchedCompoundCommit latestBcc, long latestBccSizeInBytes, long regionSizeInBytes) {
        CacheWriteCount result = new CacheWriteCount(0, 0, -1);
        if (latestBcc != null) {
            long offset = 0L;
            for (var compoundCommit : latestBcc.compoundCommits()) {
                assert offset == BlobCacheUtils.toPageAlignedSize(offset);

                // compute the number of writes in cache required
                result = getCacheWriteCount(offset, compoundCommit.headerSizeInBytes(), regionSizeInBytes, result);
                offset += compoundCommit.sizeInBytes();
                if (offset < latestBccSizeInBytes) {
                    long compoundCommitSizePageAligned = BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
                    int paddingLength = Math.toIntExact(compoundCommitSizePageAligned - compoundCommit.sizeInBytes());

                    // When assertions are enabled, extra reads are executed to assert that padding bytes at the end of the compound commit
                    // are effectively zeros (see BatchedCompoundCommit#assertPaddingComposedOfZeros). Those reads trigger more cache misses
                    // and populates the cache for regions that do not include headers, so we must account for them in this test.
                    if (Assertions.ENABLED && 0 < paddingLength) {
                        result = getCacheWriteCount(offset, paddingLength, regionSizeInBytes, result);
                    }
                    offset += paddingLength;
                }
            }
        }
        return result.writeCount;
    }

    /**
     * Computes the expected number of writes operations in cache that are required to read {@code length} bytes at {@code offset}.
     */
    private static CacheWriteCount getCacheWriteCount(long offset, long length, long regionSizeInBytes, CacheWriteCount previous) {
        int writeCount = 0;
        // region where the read operation starts
        int regionStart = (int) (offset / regionSizeInBytes);
        if (regionStart != previous.regionEnd) {
            writeCount += 1;
        }
        // offset & region where the read operation completes
        long endOffset = offset + length;
        int regionEnd;
        if (endOffset % regionSizeInBytes == 0) {
            regionEnd = (int) ((endOffset - 1L) / regionSizeInBytes);
        } else {
            regionEnd = (int) (endOffset / regionSizeInBytes);
        }
        // number of regions over which the read operation spans
        writeCount += (regionEnd - regionStart);
        return new CacheWriteCount(Math.addExact(writeCount, previous.writeCount), regionStart, regionEnd);
    }

    private static long getCommitSize(StatelessCommitRef commitRef) throws IOException {
        long sizeInBytes = 0L;
        for (var additionalFile : commitRef.getAdditionalFiles()) {
            sizeInBytes += commitRef.getDirectory().fileLength(additionalFile);
        }
        return sizeInBytes;
    }
}
