/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotDeletionsPendingExecutor.ConflictType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SnapshotDeletionsPendingExecutorTests extends ESTestCase {

    private SnapshotsService snapshotsService;
    private DeterministicTaskQueue taskQueue;
    private ClusterService clusterService;
    private TestThreadPool threadPool;
    private SnapshotDeletionsPendingExecutor executor;

    @Before
    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        snapshotsService = mock(SnapshotsService.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(2)).onResponse(null);
            return null;
        }).when(snapshotsService).deleteSnapshotsByUuid(any(), any(), any());
        taskQueue = new DeterministicTaskQueue();
        executor = new SnapshotDeletionsPendingExecutor(snapshotsService, clusterService, taskQueue.getThreadPool(), Settings.EMPTY);
        clusterService.addStateApplier(event -> executor.processPendingDeletions(event.state(), event.previousState()));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        snapshotsService = null;
        terminate(threadPool);
    }

    public void testSnapshotDeletionsPendingsAreTriggered() throws Exception {
        final RepositoryMetadata repository1 = randomRepository();
        setState(clusterService, stateWithRepository(emptyState(), repository1));

        final SnapshotId snapshot1 = randomSnapshotId();
        ClusterState state = stateWithNewPending(clusterService.state(), repository1, snapshot1, 1);
        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(true));

        setState(clusterService, state);

        assertThat(executor.isTriggered(snapshot1), is(true));
        assertThat(taskQueue.hasRunnableTasks(), is(true));

        runAllTasks();

        assertBusy(() -> assertThat(executor.isTriggered(snapshot1), is(false)));
        verify(snapshotsService, times(1)).deleteSnapshotsByUuid(eq(repository1.name()), any(), any());
        assertNoSnapshotDeletionsPendingsInClusterState();

        final RepositoryMetadata repository2 = randomRepository();
        state = stateWithRepository(clusterService.state(), repository2);
        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(false));

        setState(clusterService, state);

        final SnapshotId snapshot2 = randomSnapshotId();
        state = stateWithNewPending(clusterService.state(), repository2, snapshot2, 2);
        final SnapshotId snapshot3 = randomSnapshotId();
        state = stateWithNewPending(state, repository2, snapshot3, 3);
        final SnapshotId snapshot4 = randomSnapshotId();
        state = stateWithNewPending(state, repository2, snapshot4, 4);
        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(true));

        setState(clusterService, state);

        assertThat(executor.isTriggered(snapshot2), is(true));
        assertThat(executor.isTriggered(snapshot3), is(true));
        assertThat(executor.isTriggered(snapshot4), is(true));
        assertThat(taskQueue.hasRunnableTasks(), is(true));

        runAllTasks();

        assertBusy(() -> assertThat(executor.isTriggered(snapshot2), is(false)));
        assertThat(executor.isTriggered(snapshot3), is(false));
        assertThat(executor.isTriggered(snapshot4), is(false));
        verify(snapshotsService, times(3)).deleteSnapshotsByUuid(eq(repository2.name()), any(), any());
        assertNoSnapshotDeletionsPendingsInClusterState();
    }

    public void testSnapshotDeletionsPendingsWithRandomConflict() throws Exception {
        final RepositoryMetadata repository = randomRepository();
        ClusterState state = stateWithRepository(emptyState(), repository);

        setState(clusterService, state);

        final SnapshotId snapshot = randomSnapshotId();
        state = stateWithNewPending(state, repository, snapshot, 1L);
        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(true));
        assertThat(executor.pendingDeletionsWithConflictsChanged(state, clusterService.state()), is(false));

        final ConflictType conflict = randomFrom(ConflictType.values());
        state = addConflict(conflict, state, repository, snapshot);

        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(true));
        assertThat(executor.pendingDeletionsWithConflictsChanged(state, clusterService.state()), is(false));

        setState(clusterService, state);

        assertThat(executor.getConflict(snapshot), equalTo(conflict));
        assertThat(executor.isTriggered(snapshot), is(false));
        assertThat(taskQueue.hasRunnableTasks(), is(false));

        state = resolveConflict(conflict, state, repository, snapshot);
        assertThat(executor.pendingDeletionsChanged(state, clusterService.state()), is(false));
        assertThat(executor.pendingDeletionsWithConflictsChanged(state, clusterService.state()), is(true));

        setState(clusterService, state);

        assertThat(executor.isTriggered(snapshot), is(true));
        assertThat(taskQueue.hasRunnableTasks(), is(true));

        runAllTasks();

        assertBusy(() -> assertThat(executor.isTriggered(snapshot), is(false)));
        verify(snapshotsService, times(1)).deleteSnapshotsByUuid(any(), any(), any());
        assertNoSnapshotDeletionsPendingsInClusterState();
    }

    private void runAllTasks() {
        taskQueue.runAllTasks();
        assertThat(taskQueue.hasRunnableTasks(), is(false));
    }

    private void assertNoSnapshotDeletionsPendingsInClusterState() {
        SnapshotDeletionsPending current = clusterService.state().custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        assertThat(Strings.toString(current), current.isEmpty(), is(true));
    }

    private static ClusterState emptyState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId("_node").masterNodeId("_node"))
            .metadata(Metadata.builder().generateClusterUuidIfNeeded())
            .build();
    }

    private static RepositoryMetadata randomRepository() {
        return new RepositoryMetadata(
            randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
            UUIDs.randomBase64UUID(random()),
            "fs",
            Settings.EMPTY,
            RepositoryData.UNKNOWN_REPO_GEN,
            RepositoryData.EMPTY_REPO_GEN
        );
    }

    private static SnapshotId randomSnapshotId() {
        return new SnapshotId(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
    }

    private static ClusterState stateWithRepository(ClusterState previousState, RepositoryMetadata repositoryMetadata) {
        final RepositoriesMetadata previousRepos = previousState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        assertThat(previousRepos.repository(repositoryMetadata.name()), nullValue());
        final List<RepositoryMetadata> newRepos = new ArrayList<>(previousRepos.repositories());
        newRepos.add(repositoryMetadata);
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .metadata(Metadata.builder(previousState.metadata()).putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(newRepos)))
            .build();
    }

    private static ClusterState stateWithNewPending(
        ClusterState previousState,
        RepositoryMetadata repository,
        SnapshotId snapshot,
        long creationTime
    ) {
        SnapshotDeletionsPending previousPendings = previousState.custom(SnapshotDeletionsPending.TYPE, SnapshotDeletionsPending.EMPTY);
        SnapshotDeletionsPending.Builder pendings = new SnapshotDeletionsPending.Builder(previousPendings, e -> {});
        pendings.add(repository.name(), repository.uuid(), snapshot, creationTime);

        ImmutableOpenMap.Builder<String, ClusterState.Custom> customs = ImmutableOpenMap.builder(previousState.getCustoms());
        customs.put(SnapshotDeletionsPending.TYPE, pendings.build(Settings.EMPTY));
        return ClusterState.builder(previousState).version(previousState.version() + 1L).customs(customs.build()).build();
    }

    private static ClusterState stateWithRestore(ClusterState previousState, RepositoryMetadata repository, SnapshotId snapshot) {
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(
                RestoreInProgress.TYPE,
                new RestoreInProgress.Builder(previousState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(
                    new RestoreInProgress.Entry(
                        UUIDs.randomBase64UUID(random()),
                        new Snapshot(repository.name(), snapshot),
                        RestoreInProgress.State.INIT,
                        List.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                        null
                    )
                ).build()
            )
            .build();
    }

    private static ClusterState stateWithoutRestore(ClusterState previousState, RepositoryMetadata repository, SnapshotId snapshot) {
        final RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        boolean found = false;
        for (RestoreInProgress.Entry entry : previousState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            final Snapshot restored = entry.snapshot();
            if (restored.getRepository().equals(repository.name()) && snapshot.equals(restored.getSnapshotId())) {
                found = true;
            } else {
                builder.add(entry);
            }
        }
        assertThat("Restore not found: " + snapshot, found, is(true));
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(RestoreInProgress.TYPE, builder.build())
            .build();
    }

    private static ClusterState stateWithClone(ClusterState previousState, RepositoryMetadata repository, SnapshotId snapshot) {
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(
                SnapshotsInProgress.TYPE,
                previousState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                    .withAddedEntry(
                        SnapshotsInProgress.startClone(
                            new Snapshot(repository.name(), randomSnapshotId()),
                            snapshot,
                            Map.of(
                                randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
                                new IndexId(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()))
                            ),
                            0,
                            1,
                            Version.CURRENT
                        )
                    )
            )
            .build();
    }

    private static ClusterState stateWithoutClone(ClusterState previousState, RepositoryMetadata repository, SnapshotId snapshot) {
        final SnapshotsInProgress snapshots = previousState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
        boolean found = false;
        for (SnapshotsInProgress.Entry entry : snapshots.forRepo(repository.name())) {
            if (entry.isClone() && snapshot.equals(entry.source())) {
                found = true;
            } else {
                entries.add(entry);
            }
        }
        assertThat("Clone not found: " + snapshot, found, is(true));
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(SnapshotsInProgress.TYPE, snapshots.withUpdatedEntriesForRepo(repository.name(), entries))
            .build();
    }

    private static ClusterState stateWithRepositoryCleanUp(ClusterState previousState, RepositoryMetadata repository) {
        RepositoriesMetadata repositories = previousState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        assertThat(repositories.repository(repository.name()), notNullValue());
        RepositoryCleanupInProgress cleanUps = previousState.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY);
        assertThat(cleanUps.hasCleanupInProgress(), is(false));
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(
                RepositoryCleanupInProgress.TYPE,
                new RepositoryCleanupInProgress(List.of(RepositoryCleanupInProgress.startedEntry(repository.name(), 0L)))
            )
            .build();
    }

    private static ClusterState stateWithoutRepositoryCleanUp(ClusterState previousState, RepositoryMetadata repository) {
        RepositoriesMetadata repositories = previousState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        assertThat(repositories.repository(repository.name()), notNullValue());
        RepositoryCleanupInProgress cleanUps = previousState.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY);
        assertThat(cleanUps.hasCleanupInProgress(), is(true));
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .putCustom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY)
            .build();
    }

    private static ClusterState updateRepository(ClusterState previousState, String repositoryName, RepositoryMetadata repositoryMetadata) {
        final RepositoriesMetadata previousRepos = previousState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        assertThat(previousRepos.repository(repositoryName), notNullValue());
        final List<RepositoryMetadata> newRepos = new ArrayList<>();
        for (RepositoryMetadata r : previousRepos.repositories()) {
            if (r.name().equals(repositoryName)) {
                newRepos.add(repositoryMetadata);
            } else {
                newRepos.add(r);
            }
        }
        return ClusterState.builder(previousState)
            .version(previousState.version() + 1L)
            .metadata(Metadata.builder(previousState.metadata()).putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(newRepos)))
            .build();
    }

    private static ClusterState addConflict(
        ConflictType conflict,
        ClusterState previousState,
        RepositoryMetadata repository,
        SnapshotId snapshot
    ) {
        return switch (conflict) {
            case RESTORING -> stateWithRestore(previousState, repository, snapshot);
            case CLONING -> stateWithClone(previousState, repository, snapshot);
            case REPO_CLEANUP -> stateWithRepositoryCleanUp(previousState, repository);
            case REPO_READONLY -> updateRepository(
                previousState,
                repository.name(),
                repository.withSettings(Settings.builder().put(repository.settings()).put(READONLY_SETTING_KEY, true).build())
            );
            case REPO_MISSING -> updateRepository(
                previousState,
                repository.name(),
                new RepositoryMetadata(
                    "missing",
                    RepositoryData.MISSING_UUID,
                    "fs",
                    repository.settings(),
                    repository.generation(),
                    repository.pendingGeneration()
                )
            );
        };
    }

    private ClusterState resolveConflict(
        ConflictType conflict,
        ClusterState previousState,
        RepositoryMetadata repository,
        SnapshotId snapshot
    ) {
        return switch (conflict) {
            case RESTORING -> stateWithoutRestore(previousState, repository, snapshot);
            case CLONING -> stateWithoutClone(previousState, repository, snapshot);
            case REPO_CLEANUP -> stateWithoutRepositoryCleanUp(previousState, repository);
            case REPO_READONLY -> updateRepository(
                previousState,
                repository.name(),
                repository.withSettings(Settings.builder().put(repository.settings()).put(READONLY_SETTING_KEY, false).build())
            );
            case REPO_MISSING -> updateRepository(
                previousState,
                "missing",
                randomBoolean()
                    ? repository
                    : new RepositoryMetadata(
                        randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
                        repository.uuid(),
                        "fs",
                        repository.settings(),
                        repository.generation(),
                        repository.pendingGeneration()
                    )
            );
        };
    }
}
