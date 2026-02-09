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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContext;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessSnapshotShardContextFactoryTests extends ESTestCase {

    private TestHarness testHarness;

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (testHarness != null) {
            testHarness.close();
        }
    }

    public void testDelegateToSuperClassWhenDisabled() throws IOException {
        testHarness = createTestHarness(Settings.EMPTY, null);

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        final var snapshotShardContext = safeAwait(snapshotShardContextListener);
        assertThat(snapshotShardContext, isA(LocalPrimarySnapshotShardContext.class));
        assertFalse(testHarness.commitClosed().get());

        // Commit is released in both success and failure cases
        final var shardSnapshotResult = new ShardSnapshotResult(
            testHarness.shardGeneration(),
            ByteSizeValue.ofKb(between(1, 42)),
            between(1, 5)
        );
        if (randomBoolean()) {
            snapshotShardContext.onResponse(shardSnapshotResult);
        } else {
            snapshotShardContext.onFailure(new RuntimeException("boom"));
        }
        assertTrue(testHarness.commitClosed().get());
    }

    public void testCommitIsReleasedOnFailureToFlush() throws IOException {
        final var commitService = mock(StatelessCommitService.class);
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build(),
            commitService
        );
        final long generation = testHarness.indexCommit().getGeneration();
        if (randomBoolean()) {
            doThrow(new AlreadyClosedException("shard closed")).when(commitService)
                .ensureMaxGenerationToUploadForFlush(eq(testHarness.shardId()), eq(generation));
        } else {
            doThrow(new AlreadyClosedException("shard closed")).when(commitService)
                .addListenerForUploadedGeneration(eq(testHarness.shardId()), eq(generation), any());
        }

        try {
            testHarness.asyncCreate(new PlainActionFuture<>());
            fail("should have failed");
        } catch (AlreadyClosedException e) {
            assertTrue(testHarness.commitClosed().get());
        }
    }

    @SuppressWarnings("unchecked")
    public void testCommitIsReleasedOnCommitServiceListenerFailure() throws IOException {
        final var commitService = mock(StatelessCommitService.class);
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build(),
            commitService
        );
        final var expectedException = new RuntimeException("boom");
        final long generation = testHarness.indexCommit().getGeneration();
        doAnswer(invocation -> {
            final var listener = (ActionListener<Void>) invocation.getArgument(2);
            listener.onFailure(expectedException);
            return null;
        }).when(commitService).addListenerForUploadedGeneration(eq(testHarness.shardId()), eq(generation), anyActionListener());

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        assertThat(safeAwaitFailure(snapshotShardContextListener), sameInstance(expectedException));
        assertTrue(testHarness.commitClosed().get());
    }

    @SuppressWarnings("unchecked")
    public void testCommitIsReleasedOnCommitServiceResponseFailure() throws IOException {
        final var commitService = mock(StatelessCommitService.class);
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build(),
            commitService
        );

        final int exceptionVariant = between(0, 2);
        final long generation = testHarness.indexCommit().getGeneration();
        doAnswer(invocation -> {
            if (exceptionVariant == 0) {
                testHarness.indexShardSnapshotStatus().abortIfNotCompleted("", ignore -> {});
            }
            final var listener = (ActionListener<Void>) invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(commitService).addListenerForUploadedGeneration(eq(testHarness.shardId()), eq(generation), anyActionListener());

        final var expectedException = new RuntimeException("boom");
        if (exceptionVariant == 1) {
            when(testHarness.indexShard.store()).thenThrow(expectedException);
        } else {
            when(testHarness.indexShard.store()).thenReturn(mock(Store.class));
            when(testHarness.indexCommit.getFileNames()).thenReturn(randomList(1, 3, ESTestCase::randomIdentifier));
            when(commitService.getBlobLocation(eq(testHarness.shardId()), any())).thenThrow(expectedException);
        }

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        assertThat(
            safeAwaitFailure(snapshotShardContextListener),
            exceptionVariant == 0 ? isA(AbortedSnapshotException.class) : sameInstance(expectedException)
        );
        assertTrue(testHarness.commitClosed().get());
    }

    @SuppressWarnings("unchecked")
    public void testCommitIsReleasedOnShardSnapshotCompletion() throws IOException {
        final var commitService = mock(StatelessCommitService.class);
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build(),
            commitService
        );

        final long generation = testHarness.indexCommit().getGeneration();
        when(testHarness.indexShard.store()).thenReturn(mock(Store.class));
        when(testHarness.indexCommit.getFileNames()).thenReturn(randomList(1, 3, ESTestCase::randomIdentifier));
        when(commitService.getBlobLocation(eq(testHarness.shardId()), any())).thenReturn(
            new BlobLocation(
                new BlobFile("stateless_commit_" + generation, new PrimaryTermAndGeneration(1L, generation)),
                randomLongBetween(0, 50),
                randomLongBetween(1, 900)
            )
        );

        doAnswer(invocation -> {
            final var listener = (ActionListener<Void>) invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(commitService).addListenerForUploadedGeneration(eq(testHarness.shardId()), eq(generation), anyActionListener());

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        final var snapshotShardContext = safeAwait(snapshotShardContextListener);
        assertThat(snapshotShardContext, isA(StatelessSnapshotShardContext.class));
        assertFalse(testHarness.commitClosed().get());

        // Commit is released in both success and failure cases
        final var shardSnapshotResult = new ShardSnapshotResult(
            testHarness.shardGeneration(),
            ByteSizeValue.ofKb(between(1, 42)),
            between(1, 5)
        );
        if (randomBoolean()) {
            snapshotShardContext.onResponse(shardSnapshotResult);
        } else {
            snapshotShardContext.onFailure(new RuntimeException("boom"));
        }
        assertTrue(testHarness.commitClosed().get());
    }

    private record TestHarness(
        StatelessPlugin stateless,
        ShardId shardId,
        IndexShard indexShard,
        IndexCommit indexCommit,
        AtomicBoolean commitClosed,
        ShardGeneration shardGeneration,
        IndexShardSnapshotStatus indexShardSnapshotStatus
    ) {

        SubscribableListener<SnapshotShardContext> asyncCreate(ActionListener<ShardSnapshotResult> listener) throws IOException {
            final var factory = new StatelessSnapshotShardContextFactory(stateless());
            final var snapshot = new Snapshot(ProjectId.DEFAULT, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));

            final IndexId indexId = new IndexId(shardId().getIndexName(), randomUUID());
            final long startTime = randomNonNegativeLong();
            final IndexVersion indexVersion = IndexVersion.current();
            final ClusterState initialState = stateless().getClusterService().state();
            final var snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(
                SnapshotsInProgress.startedEntry(
                    snapshot,
                    randomBoolean(),
                    randomBoolean(),
                    Map.of(shardId().getIndexName(), indexId),
                    List.of(),
                    startTime,
                    randomNonNegativeLong(),
                    Map.of(shardId(), new SnapshotsInProgress.ShardSnapshotStatus(initialState.nodes().getLocalNodeId(), shardGeneration)),
                    Map.of(),
                    indexVersion,
                    List.of()
                )
            );
            ClusterServiceUtils.setState(
                stateless().getClusterService(),
                ClusterState.builder(initialState).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            );

            return factory.asyncCreate(shardId(), snapshot, indexId, indexShardSnapshotStatus(), indexVersion, startTime, listener);
        }

        public void close() throws IOException {
            IOUtils.close(stateless.getClusterService());
        }
    }

    private static TestHarness createTestHarness(Settings settings, StatelessCommitService commitService) {
        final var stateless = mock(StatelessPlugin.class);
        when(stateless.getCommitService()).thenReturn(commitService);

        final var clusterService = ClusterServiceUtils.createClusterService(
            new DeterministicTaskQueue().getThreadPool(),
            new ClusterSettings(settings, Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, STATELESS_SNAPSHOT_ENABLED_SETTING))
        );
        when(stateless.getClusterService()).thenReturn(clusterService);
        final var indicesService = mock(IndicesService.class);
        when(stateless.getIndicesService()).thenReturn(indicesService);

        final var shardId = new ShardId(new Index(randomIndexName(), randomUUID()), between(0, 5));

        final var indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(eq(shardId.getIndex()))).thenReturn(indexService);
        final var indexShard = mock(IndexShard.class);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);

        final String nodeId = randomIdentifier();
        final var shardRouting = ShardRoutingHelper.moveToStarted(
            ShardRoutingHelper.initialize(
                ShardRouting.newUnassigned(
                    shardId,
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
                    ShardRouting.Role.INDEX_ONLY
                ),
                nodeId
            )
        );
        when(indexShard.routingEntry()).thenReturn(shardRouting);

        final var closed = new AtomicBoolean(false);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        final IndexCommit indexCommit = mock(IndexCommit.class);
        when(indexCommit.getGeneration()).thenReturn(randomNonNegativeLong());
        when(indexShard.acquireIndexCommitForSnapshot()).thenReturn(
            new Engine.IndexCommitRef(indexCommit, () -> assertTrue(closed.compareAndSet(false, true)))
        );
        final var indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .build()
            )
            .numberOfShards(shardId.id() + 1)
            .numberOfReplicas(0)
            .build();
        final var indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        final var shardGeneration = ShardGeneration.newGeneration();
        final var shardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration);
        return new TestHarness(stateless, shardId, indexShard, indexCommit, closed, shardGeneration, shardSnapshotStatus);
    }
}
