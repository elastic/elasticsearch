/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class KeyRotationCoordinatorTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    private static byte[] randomKey() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        return keyBytes;
    }

    private static KeyEntry entry(long generatedAt) {
        return new KeyEntry(randomKey(), generatedAt);
    }

    private static DiscoveryNodes nodes(boolean isLocalMaster) {
        DiscoveryNode local = DiscoveryNodeUtils.create("local");
        DiscoveryNode other = DiscoveryNodeUtils.create("other");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(local).add(other).localNodeId("local");
        builder.masterNodeId(isLocalMaster ? "local" : "other");
        return builder.build();
    }

    private static ClusterState clusterStateWith(ProjectEncryptionKeyMetadata pek, boolean isLocalMaster) {
        return clusterStateWith(pek, null, isLocalMaster);
    }

    private static ClusterState clusterStateWith(ProjectEncryptionKeyMetadata pek, Metadata.ProjectCustom extra, boolean isLocalMaster) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (pek != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, pek);
        }
        if (extra != null) {
            project.putCustom(extra.getWriteableName(), extra);
        }
        return ClusterState.builder(CLUSTER_NAME)
            .metadata(Metadata.builder().put(project.build()).build())
            .nodes(nodes(isLocalMaster))
            .build();
    }

    /** Builds cluster state populated with a PEK per project listed in {@code pekByProject}. */
    private static ClusterState clusterStateWithProjects(Map<ProjectId, ProjectEncryptionKeyMetadata> pekByProject, boolean isLocalMaster) {
        Metadata.Builder metadata = Metadata.builder();
        for (var entry : pekByProject.entrySet()) {
            ProjectMetadata.Builder project = ProjectMetadata.builder(entry.getKey());
            if (entry.getValue() != null) {
                project.putCustom(ProjectEncryptionKeyMetadata.TYPE, entry.getValue());
            }
            metadata.put(project.build());
        }
        return ClusterState.builder(CLUSTER_NAME).metadata(metadata.build()).nodes(nodes(isLocalMaster)).build();
    }

    private KeyRotationCoordinator coordinator;
    private MasterServiceTaskQueue taskQueue;
    private EncryptionService encryptionService;

    private void setup(
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval,
        FeatureService featureService,
        List<EncryptedDataHandler<?>> handlers,
        ProjectResolver projectResolver
    ) {
        taskQueue = mock(MasterServiceTaskQueue.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.createTaskQueue(anyString(), any(), any())).thenReturn(taskQueue);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        // Run dispatched generic tasks synchronously so tests can assert on submitTask calls without async timing.
        when(threadPool.generic()).thenReturn((ExecutorService) EsExecutors.DIRECT_EXECUTOR_SERVICE);
        encryptionService = mock(EncryptionService.class);
        coordinator = KeyRotationCoordinator.create(
            clusterService,
            threadPool,
            projectResolver,
            featureService,
            encryptionService,
            handlers,
            org.elasticsearch.common.settings.Settings.builder()
                .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), rotationInterval)
                .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), checkInterval)
                .build()
        );
    }

    private void setup(
        ClusterState state,
        long now,
        TimeValue rotationInterval,
        TimeValue checkInterval,
        FeatureService featureService,
        List<EncryptedDataHandler<?>> handlers
    ) {
        setup(state, now, rotationInterval, checkInterval, featureService, handlers, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, TimeValue checkInterval, FeatureService featureService) {
        setup(state, now, rotationInterval, checkInterval, featureService, List.of());
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, TimeValue checkInterval) {
        setup(state, now, rotationInterval, checkInterval, mock(FeatureService.class));
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval, List<EncryptedDataHandler<?>> handlers) {
        setup(state, now, rotationInterval, TimeValue.timeValueMinutes(1), mock(FeatureService.class), handlers);
    }

    private void setup(ClusterState state, long now, TimeValue rotationInterval) {
        setup(state, now, rotationInterval, TimeValue.timeValueMinutes(1));
    }

    public void testInstallSubmittedWhenMasterWithoutKeyAndFeatureAvailable() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        ClusterState prev = clusterStateWith(null, true);
        ClusterState next = clusterStateWith(null, true);
        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", next, prev));

        verify(taskQueue).submitTask(
            startsWith("install-project-encryption-key"),
            argThat(
                t -> t instanceof KeyRotationCoordinator.InstallKeyTask install && install.projectId().equals(Metadata.DEFAULT_PROJECT_ID)
            ),
            any()
        );
    }

    public void testInstallNotSubmittedWhenNotMaster() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        setup(clusterStateWith(null, false), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(null, false), clusterStateWith(null, false)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenFeatureMissing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(null, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallNotSubmittedWhenKeyAlreadyExists() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        ProjectEncryptionKeyMetadata existing = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1");
        setup(clusterStateWith(existing, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(existing, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testInstallSubmittedForEachProjectMissingKey() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        Map<ProjectId, ProjectEncryptionKeyMetadata> none = new HashMap<>();
        none.put(p1, null);
        none.put(p2, null);
        ClusterState state = clusterStateWithProjects(none, true);
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state));

        verify(taskQueue).submitTask(
            startsWith("install-project-encryption-key"),
            argThat(t -> t instanceof KeyRotationCoordinator.InstallKeyTask install && install.projectId().equals(p1)),
            any()
        );
        verify(taskQueue).submitTask(
            startsWith("install-project-encryption-key"),
            argThat(t -> t instanceof KeyRotationCoordinator.InstallKeyTask install && install.projectId().equals(p2)),
            any()
        );
    }

    public void testInstallOnlySubmittedForProjectsWithoutKey() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        Map<ProjectId, ProjectEncryptionKeyMetadata> projects = new HashMap<>();
        projects.put(p1, new ProjectEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1"));
        projects.put(p2, null);
        ClusterState state = clusterStateWithProjects(projects, true);
        setup(state, 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", state, state));

        verify(taskQueue).submitTask(
            startsWith("install-project-encryption-key"),
            argThat(t -> t instanceof KeyRotationCoordinator.InstallKeyTask install && install.projectId().equals(p2)),
            any()
        );
        verify(taskQueue, never()).submitTask(
            anyString(),
            argThat(t -> t instanceof KeyRotationCoordinator.InstallKeyTask install && install.projectId().equals(p1)),
            any()
        );
    }

    public void testTickIsNoopWhenRotationDisabled() {
        setup(clusterStateWith(null, true), 0L, TimeValue.ZERO);
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNotMaster() {
        setup(clusterStateWith(null, false), 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickIsNoopWhenNoMetadataInstalled() {
        setup(clusterStateWith(null, true), 0L, TimeValue.timeValueDays(30));
        coordinator.tick();
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickBeginsRotationWhenActiveKeyIsOldEnough() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + TimeValue.timeValueDays(30).millis() + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue).submitTask(
            startsWith("begin-project-encryption-key-rotation"),
            argThat(
                t -> t instanceof KeyRotationCoordinator.BeginRotationTask begin && begin.projectId().equals(Metadata.DEFAULT_PROJECT_ID)
            ),
            any()
        );
    }

    public void testTickDoesNotBeginRotationWhenActiveKeyIsYoung() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + 1;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testRotationOnlyBeginsForProjectsWithStaleActiveKey() {
        long now = 10_000_000L;
        long fresh = now - 1;
        long stale = now - TimeValue.timeValueDays(30).millis() - 1;
        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        Map<ProjectId, ProjectEncryptionKeyMetadata> projects = new HashMap<>();
        projects.put(p1, new ProjectEncryptionKeyMetadata(Map.of("k1", entry(fresh)), "k1"));
        projects.put(p2, new ProjectEncryptionKeyMetadata(Map.of("k2", entry(stale)), "k2"));
        ClusterState state = clusterStateWithProjects(projects, true);
        setup(state, now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue).submitTask(
            startsWith("begin-project-encryption-key-rotation"),
            argThat(t -> t instanceof KeyRotationCoordinator.BeginRotationTask begin && begin.projectId().equals(p2)),
            any()
        );
        verify(taskQueue, never()).submitTask(
            anyString(),
            argThat(t -> t instanceof KeyRotationCoordinator.BeginRotationTask begin && begin.projectId().equals(p1)),
            any()
        );
    }

    public void testTickShortCircuitsHandlerAlreadyOnActiveKey() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("k1", entry(50L)),
            "k1",
            Map.of(TestCustom.TYPE, "k1")
        );
        TestCustom seeded = TestCustom.encryptedUnder("k1");
        AtomicInteger calls = new AtomicInteger();
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(captureHandler(calls, "k2")));

        coordinator.tick();

        assertEquals("handler must not be invoked when already on active key", 0, calls.get());
        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickSubmitsReEncryptApplyTaskWhenHandlerLagsActiveKey() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom seeded = TestCustom.encryptedUnder("old-key");
        AtomicInteger calls = new AtomicInteger();
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(captureHandler(calls, "k1")));

        coordinator.tick();

        assertEquals(1, calls.get());
        verify(taskQueue).submitTask(
            contains("re-encrypt-" + TestCustom.TYPE),
            isA(KeyRotationCoordinator.ReEncryptApplyTask.class),
            any()
        );
    }

    public void testHandlerReturningInputSkipsTaskSubmission() {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom seeded = TestCustom.encryptedUnder("k1");
        AtomicInteger calls = new AtomicInteger();
        EncryptedDataHandler<TestCustom> identityHandler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService svc, String activeKeyId) {
                calls.incrementAndGet();
                return current;
            }
        };
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(identityHandler));

        coordinator.tick();

        assertEquals(1, calls.get());
        verify(taskQueue, never()).submitTask(contains("re-encrypt-" + TestCustom.TYPE), any(), any());
    }

    public void testDispatchSetsProjectContextForHandler() {
        long now = 100L;
        ProjectId projectId = ProjectId.fromId("p-dispatch");
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom seeded = TestCustom.encryptedUnder("old-key");
        Map<ProjectId, ProjectEncryptionKeyMetadata> projects = Map.of(projectId, metadata);
        Metadata.Builder metaBuilder = Metadata.builder();
        ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
        project.putCustom(ProjectEncryptionKeyMetadata.TYPE, metadata);
        project.putCustom(seeded.getWriteableName(), seeded);
        metaBuilder.put(project.build());
        ClusterState state = ClusterState.builder(CLUSTER_NAME).metadata(metaBuilder.build()).nodes(nodes(true)).build();

        // Resolver that exposes the project header in the thread context; the handler observes it during reEncrypt.
        ThreadContext threadContext = new ThreadContext(org.elasticsearch.common.settings.Settings.EMPTY);
        ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        List<String> observed = new ArrayList<>();
        EncryptedDataHandler<TestCustom> handler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService svc, String activeKeyId) {
                observed.add(threadContext.getHeader(org.elasticsearch.tasks.Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER));
                return TestCustom.encryptedUnder(activeKeyId);
            }
        };
        setup(
            state,
            now,
            TimeValue.timeValueDays(30),
            TimeValue.timeValueMinutes(1),
            mock(FeatureService.class),
            List.of(handler),
            projectResolver
        );

        coordinator.tick();

        assertEquals("handler observed exactly one project context", List.of(projectId.id()), observed);
    }

    public void testTickSubmitsRetireForKeysOlderThanGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long graceMillis = 10 * checkInterval.millis();
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue).submitTask(
            startsWith("retire-project-encryption-keys"),
            eq(new KeyRotationCoordinator.RetireKeysTask(Metadata.DEFAULT_PROJECT_ID, now - graceMillis)),
            any()
        );
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(anyString(), isA(KeyRotationCoordinator.RetireKeysTask.class), any());
    }

    public void testRetireGateHonorsHandlerKeyIds() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            Map.of(TestCustom.TYPE, "old")
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(anyString(), isA(KeyRotationCoordinator.RetireKeysTask.class), any());
    }

    public void testReEncryptApplyTaskUpdatesCustomAndHandlerKeyIds() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        ActionListener<Void> listener = listenerCapturingResponses(new AtomicInteger());
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            Metadata.DEFAULT_PROJECT_ID,
            TestCustom.TYPE,
            oldCustom,
            newCustom,
            "k1",
            listener
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        ProjectState projectState = result.v1().projectState(Metadata.DEFAULT_PROJECT_ID);
        assertSame("new custom installed", newCustom, projectState.metadata().custom(TestCustom.TYPE));
        ProjectEncryptionKeyMetadata updated = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
        assertEquals("handlerKeyIds entry recorded", "k1", updated.getHandlerKeyIds().get(TestCustom.TYPE));
    }

    public void testReEncryptApplyTaskDropsOnSliceConflict() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom expectedOld = TestCustom.encryptedUnder("old-key");
        TestCustom conflictingCurrent = TestCustom.encryptedUnder("intervening-write");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, conflictingCurrent, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            Metadata.DEFAULT_PROJECT_ID,
            TestCustom.TYPE,
            expectedOld,
            newCustom,
            "k1",
            ActionListener.noop()
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        assertSame("cluster state unchanged on conflict", state, result.v1());
    }

    public void testReEncryptApplyTaskDropsOnActiveKeyDrift() throws Exception {
        long now = 100L;
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L), "k2", entry(60L)), "k2");
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustomForK1 = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            Metadata.DEFAULT_PROJECT_ID,
            TestCustom.TYPE,
            oldCustom,
            newCustomForK1,
            "k1", // computed against k1, but active is now k2
            ActionListener.noop()
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        assertSame("cluster state unchanged when activeKeyId drifted", state, result.v1());
    }

    public void testTaskForVanishedProjectIsDroppedAsNoop() throws Exception {
        long now = 100L;
        ProjectId vanished = ProjectId.fromId("vanished");
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        // Cluster state contains only DEFAULT_PROJECT_ID, not `vanished`.
        ClusterState state = clusterStateWith(metadata, true);
        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);

        Tuple<ClusterState, Void> result = executor.executeTask(new KeyRotationCoordinator.InstallKeyTask(vanished), state);
        assertSame("install task for missing project is a no-op", state, result.v1());

        result = executor.executeTask(new KeyRotationCoordinator.BeginRotationTask(vanished), state);
        assertSame("begin-rotation task for missing project is a no-op", state, result.v1());

        result = executor.executeTask(new KeyRotationCoordinator.RetireKeysTask(vanished, 0L), state);
        assertSame("retire task for missing project is a no-op", state, result.v1());
    }

    private static SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> newExecutor(long now) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        return new KeyRotationCoordinator.KeyRotationExecutor(threadPool);
    }

    private static ActionListener<Void> listenerCapturingResponses(AtomicInteger counter) {
        return ActionListener.wrap(unused -> counter.incrementAndGet(), e -> { throw new AssertionError(e); });
    }

    /**
     * Creates a handler that builds a new {@link TestCustom} pretending the data has been re-encrypted under {@code expectedKeyId}.
     */
    private static EncryptedDataHandler<TestCustom> captureHandler(AtomicInteger calls, String expectedKeyId) {
        return new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return TestCustom.TYPE;
            }

            @Override
            public TestCustom reEncrypt(TestCustom current, EncryptionService svc, String activeKeyId) {
                calls.incrementAndGet();
                return TestCustom.encryptedUnder(activeKeyId);
            }
        };
    }

    /**
     * Test-local {@link Metadata.ProjectCustom} carrying nothing but a synthetic "keyId" string. Used to seed cluster state for tests
     * that exercise the executor's CAS branches.
     */
    static final class TestCustom extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        static final String TYPE = "test_kc_custom";

        private final String keyId;

        private TestCustom(String keyId) {
            this.keyId = Objects.requireNonNull(keyId);
        }

        static TestCustom encryptedUnder(String keyId) {
            return new TestCustom(keyId);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(keyId);
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("key_id", keyId));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestCustom other && keyId.equals(other.keyId);
        }

        @Override
        public int hashCode() {
            return keyId.hashCode();
        }
    }
}
