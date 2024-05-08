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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // We check that the repository is empty after each test
public class StatelessPersistedStateTests extends ESTestCase {

    public void testClusterStateIsWrittenIntoTheStore() throws Exception {
        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);

            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));

            Set<String> indexNames = new HashSet<>();
            int writes = randomIntBetween(5, 10);
            for (int i = 0; i < writes; i++) {
                var newIndex = addRandomIndex(persistedState);
                indexNames.add(newIndex);
            }

            int deletes = randomIntBetween(0, writes);
            for (int i = deletes; i > 0; i--) {
                var indexToDelete = removeRandomIndex(persistedState);
                indexNames.remove(indexToDelete);
            }

            var persistedClusterState = getPersistedStateForTerm(persistedState, term);
            assertThat(persistedClusterState.term(), is(equalTo(term)));
            assertThat(persistedClusterState.metadata().getIndices().keySet().containsAll(indexNames), is(true));

            assertThat(
                ctx.statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(1)
                    .listBlobs(randomFrom(OperationPurpose.values())),
                is(not(emptyMap()))
            );
        }
    }

    public void testClusterStateIsOnlyWrittenByTheElectedMaster() throws Exception {
        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            final long term = 1;
            persistedState.setCurrentTerm(term);

            var lastAcceptedState = createClusterStateWithLocalNodeAsFollower(term, 1, localNode);
            persistedState.setLastAcceptedState(lastAcceptedState);

            assertThat(persistedState.getLastAcceptedState(), is(lastAcceptedState));

            Optional<PersistedClusterState> latestStoredState = PlainActionFuture.get(
                f -> persistedState.readLatestClusterStateForTerm(term, f)
            );
            assertThat(latestStoredState.isEmpty(), is(true));
        }
    }

    public void testClusterStateIsOnlyWrittenByTheElectedMasterTakeOver() throws Exception {
        var remoteNode = DiscoveryNodeUtils.create(
            "remote-node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));

            Set<String> indexNames = new HashSet<>();
            int writes = randomIntBetween(5, 10);
            for (int i = 0; i < writes; i++) {
                var newIndex = addRandomIndex(persistedState);
                indexNames.add(newIndex);
            }

            int deletes = randomIntBetween(0, writes);
            for (int i = deletes; i > 0; i--) {
                var indexToDelete = removeRandomIndex(persistedState);
                indexNames.remove(indexToDelete);
            }

            var persistedClusterStateBeforeTakeOver = getPersistedStateForTerm(persistedState, term);
            assertThat(persistedClusterStateBeforeTakeOver.term(), is(equalTo(term)));
            assertThat(persistedClusterStateBeforeTakeOver.metadata().getIndices().keySet().containsAll(indexNames), is(true));

            // remoteNode takes over
            term += 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(clusterStateWithMaster(persistedState.getLastAcceptedState(), remoteNode, term));

            int writesAfterMasterTakeOver = randomIntBetween(5, 10);
            for (int i = 0; i < writesAfterMasterTakeOver; i++) {
                var newIndex = addRandomIndex(persistedState);
                indexNames.add(newIndex);
            }

            // localNode takes over again
            term += 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(clusterStateWithMaster(persistedState.getLastAcceptedState(), localNode, term));

            int writesAfterMasterTakeOver2 = randomIntBetween(5, 10);
            for (int i = 0; i < writesAfterMasterTakeOver2; i++) {
                var newIndex = addRandomIndex(persistedState);
                indexNames.add(newIndex);
            }

            var persistedClusterStateAfterTakeOver = getPersistedStateForTerm(persistedState, term);
            assertThat(persistedClusterStateAfterTakeOver.term(), is(equalTo(term)));
            assertThat(persistedClusterStateAfterTakeOver.metadata().getIndices().keySet().containsAll(indexNames), is(true));
        }
    }

    public void testGetStoredStateWhenNoDataIsStored() throws Exception {
        try (var ctx = createTestContext()) {
            var persistedState = ctx.persistedState();
            ClusterState state = PlainActionFuture.get(f -> persistedState.getLatestStoredState(1, f));
            assertThat(state, is(nullValue()));
        }
    }

    public void testWritesUnderFailure() throws Exception {
        final boolean failWritingRegularFiles = randomBoolean();
        final boolean failWritingCommitFile = failWritingRegularFiles == false || randomBoolean();
        Function<BlobContainer, BlobContainer> wrapper = blobContainer -> new FilterBlobContainer(blobContainer) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public void writeBlob(
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (failWritingRegularFiles) {
                    throw new IOException("Failed writing blob " + blobName);
                }
                super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }

            @Override
            public void writeMetadataBlob(
                OperationPurpose purpose,
                String blobName,
                boolean failIfAlreadyExists,
                boolean atomic,
                CheckedConsumer<OutputStream, IOException> writer
            ) throws IOException {
                if (failWritingCommitFile) {
                    throw new IOException("Failed writing metadata blob " + blobName);
                }
                super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
            }
        };

        try (var ctx = createTestContext(wrapper)) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);
            expectThrows(
                Exception.class,
                () -> persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode))
            );
        }
    }

    public void testReadsUnderFailure() throws Exception {
        final boolean failReadingSegmentsFile = randomBoolean();
        final boolean failReadingOtherFiles = failReadingSegmentsFile == false || randomBoolean();
        Function<BlobContainer, BlobContainer> wrapper = blobContainer -> new FilterBlobContainer(blobContainer) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                return new FilterInputStream(super.readBlob(purpose, blobName)) {
                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        if (blobName.startsWith(IndexFileNames.SEGMENTS)) {
                            if (failReadingSegmentsFile) {
                                throw new IOException("Failed reading commit file " + blobName);
                            }
                        } else {
                            if (failReadingOtherFiles) {
                                throw new IOException("Failed reading segments file " + blobName);
                            }
                        }
                        return super.read(b, off, len);
                    }
                };
            }
        };

        try (var ctx = createTestContext(wrapper)) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<ClusterState, Exception>get(f -> persistedState.getLatestStoredState(2, f))
            );
        }
    }

    public void testTmpFilesCleanupDoNotPreventReadingClusterStateBack() throws Exception {
        final var disruptDeletesFileSystemProvider = new DisruptDeletesFileSystemProvider(PathUtils.getDefaultFileSystem());
        PathUtilsForTesting.installMock(disruptDeletesFileSystemProvider.getFileSystem(null));
        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);

            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));

            Set<String> indexNames = new HashSet<>();
            int writes = randomIntBetween(5, 10);
            for (int i = 0; i < writes; i++) {
                var newIndex = addRandomIndex(persistedState);
                indexNames.add(newIndex);
            }

            int deletes = randomIntBetween(0, writes);
            for (int i = deletes; i > 0; i--) {
                var indexToDelete = removeRandomIndex(persistedState);
                indexNames.remove(indexToDelete);
            }

            disruptDeletesFileSystemProvider.enableDeletionFailures();
            for (int i = 0; i < 2; i++) {
                var persistedClusterState = getPersistedStateForTerm(persistedState, term);
                assertThat(persistedClusterState.term(), is(equalTo(term)));
                assertThat(persistedClusterState.metadata().getIndices().keySet().containsAll(indexNames), is(true));
            }

            disruptDeletesFileSystemProvider.disableDeletionFailures();
            // The final read should clean up any leftovers from the previous reads
            getPersistedStateForTerm(persistedState, term);
        } finally {
            PathUtilsForTesting.teardown();
        }
    }

    public void testConcurrentStateDownloads() throws Exception {
        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = randomNonNegativeLong();
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));

            final var latch = new CountDownLatch(1);
            try (var listeners = new RefCountingListener(ActionTestUtils.assertNoFailureListener(ignored -> latch.countDown()))) {
                for (int i = between(1, 10); i > 0; i--) {
                    SubscribableListener

                        .<Optional<PersistedClusterState>>newForked(
                            l -> ctx.clusterStateService()
                                .getClusterStateDownloadsThreadPool()
                                .execute(ActionRunnable.wrap(l, ll -> persistedState.readLatestClusterStateForTerm(term, ll)))
                        )

                        .addListener(listeners.acquire(optionalClusterState -> {
                            final var persistedClusterState = optionalClusterState.orElseThrow(AssertionError::new);
                            assertThat(persistedClusterState.term(), is(equalTo(term)));
                        }));
                }
            }
            safeAwait(latch);

            // The final read should clean up any leftovers from the previous reads
            getPersistedStateForTerm(persistedState, term);
        }
    }

    public void testGetLatestStoredStateReturnsNullWhenAppliedStateIsFresh() throws Exception {
        try (var ctx = createTestContext()) {
            var localNode = ctx.getLocalNode();
            var persistedState = ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));
            var clusterState = PlainActionFuture.<ClusterState, Exception>get(f -> persistedState.getLatestStoredState(2, f));
            assertThat(clusterState, is(nullValue()));
        }
    }

    public void testGetLatestStoredStateReturnsTheLatestStoredStateWhenAppliedStateIsStale() throws Exception {
        try (var node1Ctx = createTestContext()) {
            var localNode = node1Ctx.getLocalNode();
            var persistedState = node1Ctx.persistedState();

            long term = 1;
            persistedState.setCurrentTerm(term);
            persistedState.setLastAcceptedState(createClusterStateWithLocalNodeAsMaster(term, 1, localNode));

            try (var node2Ctx = createTestContext(node1Ctx.statelessNode.objectStoreService)) {
                var node2PersistedState = node2Ctx.persistedState();
                node2PersistedState.setCurrentTerm(2);
                var clusterState = PlainActionFuture.<ClusterState, Exception>get(f -> node2PersistedState.getLatestStoredState(2, f));
                assertThat(clusterState, is(notNullValue()));
            }
        }
    }

    private PersistedClusterState getPersistedStateForTerm(StatelessPersistedState persistedState, long term) {
        Optional<PersistedClusterState> latestStoredState = PlainActionFuture.get(
            f -> persistedState.readLatestClusterStateForTerm(term, f)
        );
        return latestStoredState.orElseThrow(AssertionError::new);
    }

    private ClusterState clusterStateWithMaster(ClusterState oldState, DiscoveryNode newMaster, long term) {
        return ClusterState.builder(oldState)
            .nodes(DiscoveryNodes.builder(oldState.nodes()).masterNodeId(newMaster.getId()).add(newMaster).build())
            .metadata(
                Metadata.builder(oldState.metadata())
                    .coordinationMetadata(CoordinationMetadata.builder(oldState.metadata().coordinationMetadata()).term(term).build())
            )
            .incrementVersion()
            .build();
    }

    private String addRandomIndex(CoordinationState.PersistedState persistedState) {
        var lastAcceptedState = persistedState.getLastAcceptedState();

        var newIndex = getRandomIndexMetadata();
        persistedState.setLastAcceptedState(
            ClusterState.builder(lastAcceptedState)
                .metadata(Metadata.builder(lastAcceptedState.metadata()).put(newIndex, false).build())
                .incrementVersion()
                .build()
        );

        return newIndex.getIndex().getName();
    }

    private String removeRandomIndex(CoordinationState.PersistedState persistedState) {
        var lastAcceptedState = persistedState.getLastAcceptedState();

        var indexToDelete = randomFrom(lastAcceptedState.metadata().indices().keySet());

        persistedState.setLastAcceptedState(
            ClusterState.builder(lastAcceptedState)
                .metadata(Metadata.builder(lastAcceptedState.metadata()).remove(indexToDelete).build())
                .incrementVersion()
                .build()
        );

        return indexToDelete;
    }

    private ClusterState createClusterStateWithLocalNodeAsMaster(long term, long version, DiscoveryNode localNode) {
        return createClusterState(term, version, localNode, true);
    }

    private ClusterState createClusterStateWithLocalNodeAsFollower(long term, long version, DiscoveryNode localNode) {
        return createClusterState(term, version, localNode, false);
    }

    private ClusterState createClusterState(long term, long version, DiscoveryNode localNode, boolean localNodeMaster) {
        var discoveryNodes = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);
        if (localNodeMaster) {
            discoveryNodes.masterNodeId(localNode.getId());
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(version)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).clusterUUIDCommitted(true))
            .build();
    }

    private IndexMetadata getRandomIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10).toLowerCase(Locale.ROOT))
            .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())))
            .putMapping(randomMappingMetadata())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    private static MappingMetadata randomMappingMetadata() {
        if (randomBoolean()) {
            int i = randomIntBetween(1, 4);
            return new MappingMetadata(
                MapperService.SINGLE_MAPPING_NAME,
                Map.of("_doc", Map.of("properties", Map.of("field" + i, "text")))
            );
        } else {
            return null;
        }
    }

    private PersistedStateTestContext createTestContext() throws Exception {
        return createTestContext(Function.identity());
    }

    private PersistedStateTestContext createTestContext(Function<BlobContainer, BlobContainer> blobContainerWrapper) throws Exception {
        final var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return blobContainerWrapper.apply(innerContainer);
            }
        };
        final var statelessPersistedClusterStateService = new StatelessPersistedClusterStateService(
            fakeStatelessNode.nodeEnvironment,
            xContentRegistry(),
            fakeStatelessNode.clusterSettings,
            () -> 0L,
            () -> fakeStatelessNode.electionStrategy,
            () -> fakeStatelessNode.objectStoreService,
            fakeStatelessNode.threadPool,
            CompatibilityVersionsUtils.staticCurrent()
        );
        final var persistedState = statelessPersistedClusterStateService.createPersistedState(Settings.EMPTY, fakeStatelessNode.node);
        return new PersistedStateTestContext(
            fakeStatelessNode,
            statelessPersistedClusterStateService,
            (StatelessPersistedState) persistedState
        );
    }

    private PersistedStateTestContext createTestContext(ObjectStoreService objectStoreService) throws Exception {
        final var fakeStatelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry());

        final var statelessPersistedClusterStateService = new StatelessPersistedClusterStateService(
            fakeStatelessNode.nodeEnvironment,
            xContentRegistry(),
            fakeStatelessNode.clusterSettings,
            () -> 0L,
            () -> fakeStatelessNode.electionStrategy,
            () -> objectStoreService,
            fakeStatelessNode.threadPool,
            CompatibilityVersionsUtils.staticCurrent()
        );
        final var persistedState = statelessPersistedClusterStateService.createPersistedState(Settings.EMPTY, fakeStatelessNode.node);
        return new PersistedStateTestContext(
            fakeStatelessNode,
            statelessPersistedClusterStateService,
            (StatelessPersistedState) persistedState
        );
    }

    record PersistedStateTestContext(
        FakeStatelessNode statelessNode,
        StatelessPersistedClusterStateService clusterStateService,
        StatelessPersistedState persistedState
    ) implements Closeable {
        @Override
        public void close() throws IOException {
            try (var stagingPathFilesStream = Files.list(clusterStateService.getStateStagingPath())) {
                var stagingPathFiles = stagingPathFilesStream.toList();
                assertThat(stagingPathFiles.toString(), stagingPathFiles.size(), is(equalTo(0)));
            } catch (NoSuchFileException e) {
                // ignore
            }
            assertThat(
                statelessNode.objectStoreService.getClusterStateBlobContainer().listBlobs(randomFrom(OperationPurpose.values())),
                is(emptyMap())
            );
            IOUtils.close(persistedState, statelessNode);
        }

        DiscoveryNode getLocalNode() {
            return statelessNode.node;
        }
    }

    static class DisruptDeletesFileSystemProvider extends FilterFileSystemProvider {
        final AtomicBoolean injectFailures = new AtomicBoolean();

        DisruptDeletesFileSystemProvider(FileSystem inner) {
            super("disruptdeletes://", inner);
        }

        @Override
        public void delete(Path path) throws IOException {
            if (injectFailures.get()) {
                throw new IOException("Unable to delete " + path);
            } else {
                super.delete(path);
            }
        }

        void enableDeletionFailures() {
            injectFailures.set(true);
        }

        void disableDeletionFailures() {
            injectFailures.set(false);
        }
    }

}
