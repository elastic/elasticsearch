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

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.commits.StaleCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.clusterStateWithPrimaryAndSearchShards;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.markDeletedAndLocalUnused;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.markLocalUnused;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.readIndexingShardState;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.waitUntilBCCIsUploaded;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;

public class SnapshotsCommitServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private int primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(SnapshotsCommitServiceTests.class.getName(), Settings.EMPTY);
        primaryTerm = randomIntBetween(1, 100);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testLocalSnapshotRetainsCommits() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Generate and upload commits
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef commit : initialCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }

            // Set up cluster state with a running snapshot, then register the latest commit for it
            final var commitRefReleased = new AtomicBoolean(false);
            final var acquiredCommit = initialCommits.getLast();
            final var snapshotIndexCommit = new SnapshotIndexCommit(
                new Engine.IndexCommitRef(acquiredCommit, () -> commitRefReleased.set(true))
            );
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);
            testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);

            // Create a merge commit so the old commits are eligible for deletion
            final var mergedCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());
            // Mark all initial commits locally unused, all but the last of initial commits (the acquired commit) locally deleted.
            markDeletedAndLocalUnused(
                List.of(mergedCommit, acquiredCommit),
                initialCommits.subList(0, initialCommits.size() - 1),
                commitService,
                shardId
            );
            markLocalUnused(List.of(mergedCommit), List.of(acquiredCommit), commitService, shardId);

            // The registered commit should not be released due to acquired by snapshot, no deletion either
            assertFalse(commitRefReleased.get());
            assertThat(deletedCommits, empty());

            if (randomBoolean()) {
                // Simulate shard closed. Commit ref released after shard close does not trigger any more commit deletion
                commitService.closeShard(shardId);
                testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                assertThat(deletedCommits, empty());
            } else {
                // Simulate snapshot completion. The commit is released and deleted
                snapshotIndexCommit.closingBefore(ActionListener.noop()).onResponse(null);
                commitService.markCommitDeleted(shardId, acquiredCommit.getGeneration());
                assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId))));
            }
            // The commit ref is always released
            assertTrue(commitRefReleased.get());
        }
    }

    private enum AfterRecoveryAction {
        COMPLETE_SNAPSHOT,
        RELOCATE_AGAIN,
        DELETE_INDEX
    }

    public void testRecoveryRetainsRefsForRunningSnapshots() throws Exception {
        for (AfterRecoveryAction afterRecoveryAction : AfterRecoveryAction.values()) {
            logger.info("--> testing with [{}]", afterRecoveryAction);
            doTestRecoveryRetainsRefsForRunningSnapshots(afterRecoveryAction);
        }
    }

    public void doTestRecoveryRetainsRefsForRunningSnapshots(AfterRecoveryAction afterRecoveryAction) throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload initial commits (pre-recovery BCC chain)
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            // Set up cluster state with a running snapshot targeting this shard in INIT state
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            // Read state for recovery, then close and re-register the shard
            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            // Recovery should detect the running snapshots and inc refs for all recovered blob references
            commitService.markRecoveredBcc(
                shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs(),
                testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId)
            );

            // After recovery, create new commits and a merge that subsumes them
            final var postRecoveryCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 4));
            for (StatelessCommitRef commit : postRecoveryCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }
            final var mergeCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergeCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergeCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergeCommit.getGeneration());

            // Mark recovered commit and post-recovery commits as deleted and local-unused (the merge subsumes them)
            markDeletedAndLocalUnused(
                List.of(mergeCommit),
                CollectionUtils.appendToCopy(postRecoveryCommits, initialCommits.getLast()),
                commitService,
                shardId
            );

            // Post-recovery commits should be deleted because they have no snapshot refs.
            // But pre-recovery commits should still be retained for the ongoing snapshot.
            assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(postRecoveryCommits, shardId))));

            switch (afterRecoveryAction) {
                case COMPLETE_SNAPSHOT -> {
                    // Complete the snapshot via cluster state update which releases recovery refs
                    final var stateWithoutSnapshot = randomBoolean()
                        ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                        : advanceShardSnapshotState(stateWithSnapshot, snapshot, shardId);
                    ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
                    assertBusy(
                        () -> assertThat(
                            deletedCommits,
                            hasItems(staleCommits(initialCommits, shardId).toArray(StaleCompoundCommit[]::new))
                        )
                    );
                }
                case RELOCATE_AGAIN -> {
                    commitService.closeShard(shardId);
                    testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                    if (randomBoolean()) {
                        commitService.unregister(shardId);
                    }
                    // Index is not deleted, commits retained on recovery are _not_ deleted, the new primary shall handle them
                    assertThat(deletedCommits, equalTo(staleCommits(postRecoveryCommits, shardId)));
                }
                case DELETE_INDEX -> {
                    commitService.markIndexDeleting(List.of(shardId));
                    commitService.closeShard(shardId);
                    testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
                    commitService.delete(shardId);
                    if (randomBoolean()) {
                        commitService.unregister(shardId);
                    }
                    // If the index is being deleted, commits retained on recovery are also deleted
                    assertBusy(
                        () -> assertThat(
                            deletedCommits,
                            hasItems(staleCommits(initialCommits, shardId).toArray(StaleCompoundCommit[]::new))
                        )
                    );
                }
            }
        }
    }

    public void testRemoteSnapshotRefsReplacedForTheSameSnapshot() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var deletedCommits = fixture.deletedCommits;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            // Create and upload initial commits (pre-recovery BCC chain)
            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            // Set up cluster state with a running snapshot, then perform recovery which retains all commits for the snapshot
            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            commitService.markRecoveredBcc(
                shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs(),
                testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId)
            );

            // After recovery, create a merge commit
            final var mergeCommit = testHarness.generateIndexCommits(1, true).getFirst();
            commitService.onCommitCreation(mergeCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergeCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergeCommit.getGeneration());

            // Mark recovered BCC as deleted and local-unused, no deletion yet due to snapshot
            markDeletedAndLocalUnused(List.of(mergeCommit), List.of(initialCommits.getLast()), commitService, shardId);
            assertThat(deletedCommits, empty());

            // Register the same snapshot for the merge commit via registerReleaseForSnapshot.
            // This replaces the broad recovery ref with one specific for the merge commit,
            // allowing the pre-recovery commits to be deleted.
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(mergeCommit, () -> {}));
            testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);

            assertBusy(() -> assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId))));
        }
    }

    public void testRaceBetweenSnapshotRegisterAndShardClosed() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            final var snapshot = randomSnapshot();
            final var commitRefReleased = new AtomicBoolean(false);
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(commit, () -> commitRefReleased.set(true)));

            // Race between concurrent snapshot register and shard closing
            final var barrier = new CyclicBarrier(2);
            final var registerThread = new Thread(() -> {
                safeAwait(barrier);
                testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
            });
            final var releaseThread = new Thread(() -> {
                safeAwait(barrier);
                commitService.closeShard(shardId);
                testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            });

            registerThread.start();
            releaseThread.start();
            registerThread.join(30_000);
            releaseThread.join(30_000);
            assertTrue("commit ref should be released after concurrent register and shard close", commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testRaceBetweenSnapshotRegisterAndSnapshotCompletion() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(1);
            final var commit = commits.getFirst();
            commitService.onCommitCreation(commit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            final var stateWithoutSnapshot = randomBoolean()
                ? removeSnapshotFromClusterState(stateWithSnapshot, snapshot)
                : failShardSnapshotState(stateWithSnapshot, snapshot, shardId);

            final var commitRefReleased = new AtomicBoolean(false);
            final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(commit, () -> commitRefReleased.set(true)));

            // Race between a lagging remote node registering and the snapshot completing/failing
            final var barrier = new CyclicBarrier(2);
            final var registerThread = new Thread(() -> {
                safeAwait(barrier);
                testHarness.snapshotsCommitService.registerReleaseForSnapshot(shardId, snapshot, snapshotIndexCommit);
            });
            final var completionThread = new Thread(() -> {
                safeAwait(barrier);
                ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
            });

            registerThread.start();
            completionThread.start();
            registerThread.join(30_000);
            completionThread.join(30_000);
            assertTrue("commit ref should be released after concurrent register and snapshot completion", commitRefReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    public void testRaceBetweenRetainingCommitsOnRecoveryAndSnapshotCompletion() throws Exception {
        try (var fixture = createTestFixture()) {
            final var testHarness = fixture.node;
            final var shardId = testHarness.shardId;
            final var commitService = testHarness.commitService;

            final var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(2, 5));
            for (StatelessCommitRef commit : initialCommits) {
                commitService.onCommitCreation(commit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            }

            final var snapshot = randomSnapshot();
            final var stateWithSnapshot = createClusterStateWithSnapshotsInProgress(testHarness, snapshot);
            ClusterServiceUtils.setState(testHarness.clusterService, stateWithSnapshot);

            // Close, unregister, and re-register the shard to simulate a recovery
            final var indexingShardState = readIndexingShardState(testHarness, primaryTerm);
            commitService.closeShard(shardId);
            testHarness.snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            commitService.unregister(shardId);
            commitService.register(
                shardId,
                primaryTerm,
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> gcpListener.accept(Long.MAX_VALUE, null),
                () -> {}
            );

            // Race between concurrent recovery and snapshot completion
            final var releasableReleased = new AtomicBoolean(false);
            final var extraReferenceConsumers = new ArrayList<Consumer<Releasable>>();
            testHarness.snapshotsCommitService.getExtraReferenceConsumers(shardId).forEachRemaining(extraReferenceConsumers::add);
            assertThat(extraReferenceConsumers.size(), equalTo(1));
            final Iterator<Consumer<Releasable>> consumerIterator = Iterators.map(extraReferenceConsumers.iterator(), c -> r -> {
                c.accept(Releasables.releaseOnce(() -> {
                    releasableReleased.set(true);
                    Releasables.close(r);
                }));
            });
            final var stateWithoutSnapshot = removeSnapshotFromClusterState(stateWithSnapshot, snapshot);

            final var barrier = new CyclicBarrier(2);
            final var recoveryThread = new Thread(() -> {
                safeAwait(barrier);
                commitService.markRecoveredBcc(
                    shardId,
                    indexingShardState.latestCommit(),
                    indexingShardState.otherBlobs(),
                    consumerIterator
                );
            });
            final var clusterChangedThread = new Thread(() -> {
                safeAwait(barrier);
                ClusterServiceUtils.setState(testHarness.clusterService, stateWithoutSnapshot);
            });

            recoveryThread.start();
            clusterChangedThread.start();
            recoveryThread.join(30_000);
            clusterChangedThread.join(30_000);
            assertTrue("releasable should be released after concurrent recovery and snapshot completion", releasableReleased.get());
            assertFalse(testHarness.snapshotsCommitService.hasTrackingForShard(shardId));
        }
    }

    private record TestFixture(FakeStatelessNode node, Set<StaleCompoundCommit> deletedCommits) implements Closeable {
        @Override
        public void close() throws IOException {
            node.close();
        }
    }

    private TestFixture createTestFixture() throws IOException {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        final var commitCleaner = new StatelessCommitCleaner(null, null, mock(ObjectStoreService.class)) {
            @Override
            protected void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        final var stateRef = new AtomicReference<ClusterState>();
        final var node = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId));
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        };
        stateRef.set(clusterStateWithPrimaryAndSearchShards(node.shardId, 0));
        return new TestFixture(node, deletedCommits);
    }

    private ClusterState createClusterStateWithSnapshotsInProgress(FakeStatelessNode testHarness, Snapshot... snapshots) {
        var snapshotsInProgress = SnapshotsInProgress.EMPTY;
        for (final var snapshot : snapshots) {
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                SnapshotsInProgress.startedEntry(
                    snapshot,
                    randomBoolean(),
                    randomBoolean(),
                    Map.of(testHarness.shardId.getIndexName(), new IndexId(testHarness.shardId.getIndexName(), randomUUID())),
                    List.of(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    Map.of(
                        testHarness.shardId,
                        new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), SnapshotsInProgress.ShardState.INIT, null)
                    ),
                    Map.of(),
                    IndexVersion.current(),
                    List.of()
                )
            );
        }
        return ClusterState.builder(testHarness.clusterService.state()).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();
    }

    private static ClusterState removeSnapshotFromClusterState(ClusterState clusterState, Snapshot snapshot) {
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterState);
        return ClusterState.builder(clusterState)
            .putCustom(
                SnapshotsInProgress.TYPE,
                snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                    snapshot.getProjectId(),
                    snapshot.getRepository(),
                    snapshotsInProgress.forRepo(snapshot.getProjectId(), snapshot.getRepository())
                        .stream()
                        .filter(entry -> snapshot.equals(entry.snapshot()) == false)
                        .toList()
                )
            )
            .build();
    }

    private ClusterState failShardSnapshotState(ClusterState clusterState, Snapshot snapshot, ShardId shardId) {
        return updateShardSnapshotStatus(
            clusterState,
            snapshot,
            shardId,
            new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), SnapshotsInProgress.ShardState.FAILED, null, "failed")
        );
    }

    private ClusterState advanceShardSnapshotState(ClusterState clusterState, Snapshot snapshot, ShardId shardId) {
        return updateShardSnapshotStatus(
            clusterState,
            snapshot,
            shardId,
            SnapshotsInProgress.ShardSnapshotStatus.success(
                randomIdentifier(),
                new ShardSnapshotResult(new ShardGeneration(randomIdentifier()), ByteSizeValue.ZERO, 0)
            )
        );
    }

    private ClusterState updateShardSnapshotStatus(
        ClusterState clusterState,
        Snapshot snapshot,
        ShardId shardId,
        SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus
    ) {
        final var snapshotsInProgress = SnapshotsInProgress.get(clusterState);
        final var entry = snapshotsInProgress.snapshot(snapshot);
        assert entry != null;
        final var updatedEntry = SnapshotsInProgress.startedEntry(
            snapshot,
            entry.includeGlobalState(),
            entry.partial(),
            entry.indices(),
            entry.dataStreams(),
            entry.startTime(),
            entry.repositoryStateId(),
            Map.of(shardId, shardSnapshotStatus),
            entry.userMetadata(),
            entry.version(),
            entry.featureStates()
        );
        return ClusterState.builder(clusterState)
            .putCustom(
                SnapshotsInProgress.TYPE,
                snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(
                    snapshot.getProjectId(),
                    snapshot.getRepository(),
                    snapshotsInProgress.forRepo(snapshot.getProjectId(), snapshot.getRepository())
                        .stream()
                        .map(e -> e.snapshot().equals(snapshot) ? updatedEntry : e)
                        .toList()
                )
            )
            .build();
    }

    private static Snapshot randomSnapshot() {
        return new Snapshot(ProjectId.DEFAULT, randomRepoName(), new SnapshotId(randomSnapshotName(), randomUUID()));
    }

    private Set<StaleCompoundCommit> staleCommits(List<StatelessCommitRef> commits, ShardId shardId) {
        return commits.stream().map(commit -> staleCommit(shardId, commit)).collect(Collectors.toSet());
    }

    private StaleCompoundCommit staleCommit(ShardId shardId, StatelessCommitRef commit) {
        return new StaleCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()), primaryTerm);
    }
}
