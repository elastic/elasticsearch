/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.FinalizeSnapshotContext.UpdatedShardGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSLMGetExpiredSnapshotsActionTests extends ESTestCase {

    public void testEmpty() {
        runActionTest(List.of(), Set.of());
    }

    public void testNoneExpire() {
        runActionTest(List.of(mkInfo("keep", POLICY_ID)), Set.of());
    }

    public void testOneExpires() {
        runActionTest(List.of(mkInfo("keep", POLICY_ID), mkInfo("expires", POLICY_ID)), Set.of("expires"));
    }

    public void testNoPolicy() {
        runActionTest(List.of(mkInfo("keep", null)), Set.of());
    }

    public void testUnknownPolicy() {
        runActionTest(List.of(mkInfo("keep", "unknown-policy")), Set.of());
    }

    public void testPolicyWithoutRetention() {
        runActionTest(List.of(mkInfo("keep", NO_RETENTION_POLICY_ID)), Set.of());
    }

    public void testWrongRepoPolicy() {
        runActionTest(List.of(mkInfo("keep", OTHER_REPO_POLICY_ID)), Set.of());
    }

    private static SnapshotId mkId(String snapshotId) {
        return new SnapshotId(snapshotId, snapshotId);
    }

    private static SnapshotInfo mkInfo(String snapshotId, @Nullable String slmPolicy) {
        return new SnapshotInfo(
            new Snapshot(REPO_NAME, mkId(snapshotId)),
            List.of(),
            List.of(),
            List.of(),
            null,
            IndexVersion.current(),
            2L,
            2L,
            0,
            0,
            List.of(),
            false,
            slmPolicy == null ? Map.of() : Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, slmPolicy),
            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL),
            Map.of()
        );
    }

    private static final String REPO_NAME = "repo";
    private static final String POLICY_ID = "policy";
    private static final String NO_RETENTION_POLICY_ID = "no-retention-policy";
    private static final String OTHER_REPO_POLICY_ID = "other-repo-policy";

    private static void runActionTest(List<SnapshotInfo> snapshotInfos, Set<String> snapshotUuidsToDelete) {
        final var snapshotsToDelete = snapshotUuidsToDelete.stream().map(i -> new SnapshotId(i, i)).collect(Collectors.toSet());

        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getTaskManager()).thenReturn(null);

        final var repository = createMockRepository(threadPool, snapshotInfos);

        final var repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(anyString())).then(invocation -> {
            final String requestedRepoName = invocation.getArgument(0);
            if (REPO_NAME.equals(requestedRepoName)) {
                return repository;
            } else {
                throw new RepositoryMissingException(requestedRepoName);
            }
        });

        final var action = new TransportSLMGetExpiredSnapshotsAction(transportService, repositoriesService, new ActionFilters(Set.of()));
        final var task = new Task(1, "direct", TransportSLMGetExpiredSnapshotsAction.INSTANCE.name(), "", TaskId.EMPTY_TASK_ID, Map.of());

        final var policyMap = createPolicies(
            snapshotInfos.stream()
                .filter(s -> POLICY_ID.equals(RepositoryData.SnapshotDetails.fromSnapshotInfo(s).getSlmPolicy()))
                .map(SnapshotInfo::snapshotId)
                .collect(Collectors.toSet()),
            snapshotsToDelete
        );

        final var responseFuture = new PlainActionFuture<TransportSLMGetExpiredSnapshotsAction.Response>();
        action.doExecute(task, new TransportSLMGetExpiredSnapshotsAction.Request(List.of(REPO_NAME), policyMap), responseFuture);
        deterministicTaskQueue.runAllTasks();
        assertTrue(responseFuture.isDone());
        final var deletedSnapshots = responseFuture.actionGet().snapshotsToDelete();
        if (snapshotsToDelete.isEmpty()) {
            assertThat(deletedSnapshots, anEmptyMap());
        } else {
            assertThat(deletedSnapshots.keySet(), equalTo(Set.of(REPO_NAME)));
            final var repoDeletedSnapshots = deletedSnapshots.get(REPO_NAME);
            assertEquals(snapshotsToDelete, repoDeletedSnapshots.stream().map(Tuple::v1).collect(Collectors.toSet()));
            assertEquals(Set.of(POLICY_ID), repoDeletedSnapshots.stream().map(Tuple::v2).collect(Collectors.toSet()));
        }
    }

    public void testGetSnapshotDetailsByPolicy() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final Set<String> snapshotIds = randomSet(0, 10, () -> randomAlphaOfLength(10));
        final var policyNames = new String[] { "policy-1", "policy-2", "policy-3" };
        final var snapshotInfos = snapshotIds.stream().map(s -> mkInfo(s, randomFrom(policyNames))).toList();
        final var repository = createMockRepository(deterministicTaskQueue.getThreadPool(), snapshotInfos);

        record SeenSnapshotInfo(SnapshotId snapshotId, String policyId) {}
        final var seenSnapshotInfos = snapshotInfos.stream()
            .map(si -> new SeenSnapshotInfo(si.snapshotId(), RepositoryData.SnapshotDetails.fromSnapshotInfo(si).getSlmPolicy()))
            .collect(Collectors.toSet());

        final var testListener = SubscribableListener

            .<RepositoryData>newForked(l -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l))

            .<TransportSLMGetExpiredSnapshotsAction.SnapshotDetailsByPolicy>andThen(
                (l, rd) -> TransportSLMGetExpiredSnapshotsAction.getSnapshotDetailsByPolicy(
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    repository,
                    rd,
                    l
                )
            )

            .andThenAccept(snapshotDetailsByPolicy -> {
                snapshotDetailsByPolicy.flatMap((policyId, snapshotsMap) -> snapshotsMap.entrySet().stream().map(entry -> {
                    assertThat(policyId, oneOf(policyNames));
                    assertEquals(policyId, entry.getValue().getSlmPolicy());
                    return new SeenSnapshotInfo(entry.getKey(), policyId);
                })).forEach(seenSnapshotInfo -> assertTrue(seenSnapshotInfos.remove(seenSnapshotInfo)));
            });

        deterministicTaskQueue.runAllTasks();
        assertTrue(testListener.isDone());
        assertThat(seenSnapshotInfos, empty());
    }

    public void testGetSnapshotsToDelete() {
        final var snapshotDetailsByPolicy = new TransportSLMGetExpiredSnapshotsAction.SnapshotDetailsByPolicy();

        assertEquals(
            List.of(),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(), Set.of()),
                snapshotDetailsByPolicy
            )
        );

        snapshotDetailsByPolicy.add(mkId("snapshot-with-unknown-policy"), mkDetails("unknown-policy-id"));

        assertEquals(
            List.of(),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(), Set.of()),
                snapshotDetailsByPolicy
            )
        );

        snapshotDetailsByPolicy.add(mkId("no-retention"), mkDetails(NO_RETENTION_POLICY_ID));

        assertEquals(
            List.of(),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(), Set.of()),
                snapshotDetailsByPolicy
            )
        );

        snapshotDetailsByPolicy.add(mkId("other-repo-policy"), mkDetails(OTHER_REPO_POLICY_ID));

        assertEquals(
            List.of(),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(), Set.of()),
                snapshotDetailsByPolicy
            )
        );

        snapshotDetailsByPolicy.add(mkId("expiry-candidate"), mkDetails(POLICY_ID));

        assertEquals(
            List.of(),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(mkId("expiry-candidate")), Set.of()),
                snapshotDetailsByPolicy
            )
        );

        assertEquals(
            List.of(Tuple.tuple(mkId("expiry-candidate"), POLICY_ID)),
            TransportSLMGetExpiredSnapshotsAction.getSnapshotsToDelete(
                REPO_NAME,
                createPolicies(Set.of(mkId("expiry-candidate")), Set.of(mkId("expiry-candidate"))),
                snapshotDetailsByPolicy
            )
        );
    }

    private static RepositoryData.SnapshotDetails mkDetails(String policyId) {
        return new RepositoryData.SnapshotDetails(
            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL),
            IndexVersion.current(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            policyId
        );
    }

    private static Repository createMockRepository(ThreadPool threadPool, List<SnapshotInfo> snapshotInfos) {
        final var repository = mock(Repository.class);
        doAnswer(invocation -> {
            final ActionListener<RepositoryData> listener = invocation.getArgument(1);
            threadPool.generic().execute(ActionRunnable.supply(listener, () -> {
                var repositoryData = RepositoryData.EMPTY;
                for (SnapshotInfo snapshotInfo : snapshotInfos) {
                    var snapshotDetails = RepositoryData.SnapshotDetails.fromSnapshotInfo(snapshotInfo);
                    if (randomBoolean()) {
                        snapshotDetails = new RepositoryData.SnapshotDetails(
                            snapshotDetails.getSnapshotState(),
                            snapshotDetails.getVersion(),
                            snapshotDetails.getStartTimeMillis(),
                            snapshotDetails.getEndTimeMillis(),
                            null
                        );
                    }
                    repositoryData = repositoryData.addSnapshot(
                        snapshotInfo.snapshotId(),
                        snapshotDetails,
                        UpdatedShardGenerations.EMPTY,
                        Map.of(),
                        Map.of()
                    );
                }
                return repositoryData;
            }));
            return null;
        }).when(repository).getRepositoryData(any(), any());

        doAnswer(invocation -> {
            final Collection<SnapshotId> snapshotIdCollection = invocation.getArgument(0);
            assertFalse("should not abort on failure", invocation.getArgument(1));
            final CheckedConsumer<SnapshotInfo, Exception> consumer = invocation.getArgument(3);
            final ActionListener<Void> listener = invocation.getArgument(4);

            try (var refs = new RefCountingRunnable(() -> listener.onResponse(null))) {
                final Set<SnapshotId> snapshotIds = new HashSet<>(snapshotIdCollection);
                for (SnapshotInfo snapshotInfo : snapshotInfos) {
                    if (snapshotIds.remove(snapshotInfo.snapshotId())) {
                        threadPool.generic().execute(ActionRunnable.run(refs.acquireListener(), () -> {
                            try {
                                consumer.accept(snapshotInfo);
                            } catch (Exception e) {
                                fail(e);
                            }
                        }));
                    }
                }
            }
            return null;
        }).when(repository).getSnapshotInfo(any(), anyBoolean(), any(), any(), any());

        doAnswer(invocation -> new RepositoryMetadata(REPO_NAME, "test", Settings.EMPTY)).when(repository).getMetadata();

        return repository;
    }

    private static Map<String, SnapshotLifecyclePolicy> createPolicies(
        Set<SnapshotId> expectedSnapshotIds,
        Set<SnapshotId> snapshotsToDelete
    ) {
        return Stream.of(
            new SnapshotLifecyclePolicy(
                POLICY_ID,
                "snap",
                "1 * * * * ?",
                REPO_NAME,
                null,
                new SnapshotRetentionConfiguration(null, null, null) {
                    @Override
                    public boolean isSnapshotEligibleForDeletion(
                        SnapshotId snapshotId,
                        RepositoryData.SnapshotDetails snapshotDetails,
                        Map<SnapshotId, RepositoryData.SnapshotDetails> allSnapshots
                    ) {
                        assertEquals(allSnapshots.keySet(), expectedSnapshotIds);
                        assertEquals(POLICY_ID, snapshotDetails.getSlmPolicy());
                        return snapshotsToDelete.contains(snapshotId);
                    }
                }
            ),
            new SnapshotLifecyclePolicy(
                OTHER_REPO_POLICY_ID,
                "snap",
                "1 * * * * ?",
                "other-repo",
                null,
                new SnapshotRetentionConfiguration(null, null, null) {
                    @Override
                    public boolean isSnapshotEligibleForDeletion(
                        SnapshotId snapshotId,
                        RepositoryData.SnapshotDetails snapshotDetails,
                        Map<SnapshotId, RepositoryData.SnapshotDetails> allSnapshots
                    ) {
                        throw new AssertionError("should not be called");
                    }
                }
            ),
            new SnapshotLifecyclePolicy(
                NO_RETENTION_POLICY_ID,
                "snap",
                "1 * * * * ?",
                "other-repo",
                null,
                randomBoolean() ? null : SnapshotRetentionConfiguration.EMPTY
            )
        ).collect(Collectors.toMap(SnapshotLifecyclePolicy::getId, p -> p));
    }
}
