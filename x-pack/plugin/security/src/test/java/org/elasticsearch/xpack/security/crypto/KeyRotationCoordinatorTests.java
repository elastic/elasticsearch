/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.security.crypto.PrimaryEncryptionKeyMetadata.KeyEntry;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandler;
import org.elasticsearch.xpack.security.spi.encryption.EncryptionService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
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

    private static ClusterState clusterStateWith(PrimaryEncryptionKeyMetadata pek, boolean isLocalMaster) {
        return clusterStateWith(pek, null, isLocalMaster);
    }

    private static ClusterState clusterStateWith(PrimaryEncryptionKeyMetadata pek, Metadata.ProjectCustom extra, boolean isLocalMaster) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (pek != null) {
            project.putCustom(PrimaryEncryptionKeyMetadata.TYPE, pek);
        }
        if (extra != null) {
            project.putCustom(extra.getWriteableName(), extra);
        }
        return ClusterState.builder(CLUSTER_NAME)
            .metadata(Metadata.builder().put(project.build()).build())
            .nodes(nodes(isLocalMaster))
            .build();
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
        List<EncryptedDataHandler<?>> handlers
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
            DefaultProjectResolver.INSTANCE,
            featureService,
            encryptionService,
            handlers,
            org.elasticsearch.common.settings.Settings.builder()
                .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), rotationInterval)
                .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), checkInterval)
                .build()
        );
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

        verify(taskQueue).submitTask(eq("install-primary-encryption-key"), isA(KeyRotationCoordinator.InstallKeyTask.class), any());
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
        PrimaryEncryptionKeyMetadata existing = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(0L)), "k1");
        setup(clusterStateWith(existing, true), 0L, TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(1), featureService);

        coordinator.onClusterStateChanged(new ClusterChangedEvent("test", clusterStateWith(existing, true), clusterStateWith(null, true)));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
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
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("begin-primary-encryption-key-rotation"),
            isA(KeyRotationCoordinator.BeginRotationTask.class),
            any()
        );
    }

    public void testTickDoesNotBeginRotationWhenActiveKeyIsYoung() {
        long generatedAt = 1_000_000_000L;
        long now = generatedAt + 1;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(generatedAt)), "k1");
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30));

        coordinator.tick();

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testTickShortCircuitsHandlerAlreadyOnActiveKey() {
        long now = 100L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
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
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom seeded = TestCustom.encryptedUnder("old-key");
        AtomicInteger calls = new AtomicInteger();
        setup(clusterStateWith(metadata, seeded, true), now, TimeValue.timeValueDays(30), List.of(captureHandler(calls, "k1")));

        coordinator.tick();

        assertEquals(1, calls.get());
        verify(taskQueue).submitTask(eq("re-encrypt-" + TestCustom.TYPE), isA(KeyRotationCoordinator.ReEncryptApplyTask.class), any());
    }

    public void testHandlerReturningInputSkipsTaskSubmission() {
        long now = 100L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
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
        verify(taskQueue, never()).submitTask(eq("re-encrypt-" + TestCustom.TYPE), any(), any());
    }

    public void testTickSubmitsRetireForKeysOlderThanGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long graceMillis = 10 * checkInterval.millis();
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue).submitTask(
            eq("retire-primary-encryption-keys"),
            eq(new KeyRotationCoordinator.RetireKeysTask(now - graceMillis)),
            any()
        );
    }

    public void testTickDoesNotSubmitRetireWhenAllKeysWithinGrace() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 60 * 1000L;
        long oldGeneratedAt = now - 5 * 60 * 1000L;

        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active"
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(
            eq("retire-primary-encryption-keys"),
            isA(KeyRotationCoordinator.RetireKeysTask.class),
            any()
        );
    }

    public void testRetireGateHonorsHandlerKeyIds() {
        TimeValue checkInterval = TimeValue.timeValueMinutes(1);
        long now = 1_000_000L;
        long activeGeneratedAt = now - 15 * 60 * 1000L;
        long oldGeneratedAt = now - 30 * 60 * 1000L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(
            Map.of("old", entry(oldGeneratedAt), "active", entry(activeGeneratedAt)),
            "active",
            Map.of(TestCustom.TYPE, "old")
        );
        setup(clusterStateWith(metadata, true), now, TimeValue.timeValueDays(30), checkInterval);

        coordinator.tick();

        verify(taskQueue, never()).submitTask(
            eq("retire-primary-encryption-keys"),
            isA(KeyRotationCoordinator.RetireKeysTask.class),
            any()
        );
    }

    public void testReEncryptApplyTaskUpdatesCustomAndHandlerKeyIds() throws Exception {
        long now = 100L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        ActionListener<Void> listener = listenerCapturingResponses(new AtomicInteger());
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            TestCustom.TYPE,
            oldCustom,
            newCustom,
            "k1",
            listener
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        ProjectState projectState = DefaultProjectResolver.INSTANCE.getProjectState(result.v1());
        assertSame("new custom installed", newCustom, projectState.metadata().custom(TestCustom.TYPE));
        PrimaryEncryptionKeyMetadata updated = projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);
        assertEquals("handlerKeyIds entry recorded", "k1", updated.getHandlerKeyIds().get(TestCustom.TYPE));
    }

    public void testReEncryptApplyTaskDropsOnSliceConflict() throws Exception {
        long now = 100L;
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L)), "k1");
        TestCustom expectedOld = TestCustom.encryptedUnder("old-key");
        TestCustom conflictingCurrent = TestCustom.encryptedUnder("intervening-write");
        TestCustom newCustom = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, conflictingCurrent, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
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
        PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of("k1", entry(50L), "k2", entry(60L)), "k2");
        TestCustom oldCustom = TestCustom.encryptedUnder("old-key");
        TestCustom newCustomForK1 = TestCustom.encryptedUnder("k1");
        ClusterState state = clusterStateWith(metadata, oldCustom, true);

        SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> executor = newExecutor(now);
        KeyRotationCoordinator.ReEncryptApplyTask task = new KeyRotationCoordinator.ReEncryptApplyTask(
            TestCustom.TYPE,
            oldCustom,
            newCustomForK1,
            "k1", // computed against k1, but active is now k2
            ActionListener.noop()
        );

        Tuple<ClusterState, Void> result = executor.executeTask(task, state);

        assertSame("cluster state unchanged when activeKeyId drifted", state, result.v1());
    }

    private static SimpleBatchedExecutor<KeyRotationCoordinator.KeyRotationTask, Void> newExecutor(long now) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.absoluteTimeInMillis()).thenReturn(now);
        return new KeyRotationCoordinator.KeyRotationExecutor(DefaultProjectResolver.INSTANCE, threadPool);
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
