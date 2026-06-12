/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class KeyRotationIT extends SecurityIntegTestCase {

    private final List<Integer> keyAddEvents = new CopyOnWriteArrayList<>();

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // The encryption settings are only registered when the feature flag is enabled
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()) {
            builder.put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(5))
                .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1));
            SecuritySettingsSource.addSecureSettings(builder, secure -> {
                secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v1");
                secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1", "encryption-test-password");
            });
        }
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(TestEncryptionCustomsPlugin.class);
        return plugins;
    }

    @Before
    public void setup() throws Exception {
        // Check feature flag
        assumeTrue(
            "project encryption key feature flag must be enabled",
            ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
        // Add listener that registers PEK changes
        internalCluster().clusterService().addListener(event -> {
            if (event.changedCustomProjectMetadataSet().contains(ProjectEncryptionKeyMetadata.TYPE) == false) {
                return;
            }
            ProjectEncryptionKeyMetadata curr = event.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
            if (curr == null) {
                return;
            }
            ProjectEncryptionKeyMetadata prev = event.previousState().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
            Set<String> prevKeys = prev != null ? prev.getKeys().keySet() : Set.of();
            var diff = Sets.difference(curr.getKeys().keySet(), prevKeys);
            if (!diff.isEmpty()) {
                keyAddEvents.add(diff.size());
            }
        });
        ensureGreen();
        // Wait for first PEK install
        assertBusy(() -> assertThat(metadataOnMaster(), notNullValue()));
    }

    private ProjectEncryptionKeyMetadata metadataOnMaster() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private EncryptionService masterEncryptionService() {
        return internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
    }

    private <T extends TestBlobBase> T customOnMaster(String customName) {
        String master = internalCluster().getMasterName();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        ProjectResolver projectResolver = internalCluster().getInstance(ProjectResolver.class, master);
        return projectResolver.getProjectState(clusterService.state()).metadata().custom(customName);
    }

    /**
     * Registers a {@link TestHandler} on every node's coordinator. All registrations share a single {@link TestControlState}, so the
     * handler that runs on whichever node is currently master mutates the same in-JVM state that the test reads.
     */
    private <T extends TestBlobBase> TestControlState registerHandlerOnAllNodes(String customName, Function<EncryptedData, T> factory) {
        final TestControlState state = new TestControlState();
        for (String nodeName : internalCluster().getNodeNames()) {
            KeyRotationCoordinator coordinator = internalCluster().getInstance(KeyRotationCoordinator.class, nodeName);
            EncryptionService service = internalCluster().getInstance(EncryptionService.class, nodeName);
            coordinator.register(new TestHandler<>(customName, factory, service, state));
        }
        return state;
    }

    /**
     * Seeds {@code blob} into the master's project state at {@code customName} via an unbatched cluster-state update task, and waits
     * for it to be applied.
     */
    private void seedBlob(String customName, TestBlobBase blob) throws Exception {
        String master = internalCluster().getMasterName();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        ProjectResolver projectResolver = internalCluster().getInstance(ProjectResolver.class, master);
        CountDownLatch applied = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        clusterService.submitUnbatchedStateUpdateTask("seed-" + customName, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateProject(
                    projectResolver.getProjectState(currentState).projectId(),
                    b -> b.putCustom(customName, blob)
                );
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                applied.countDown();
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                applied.countDown();
            }
        });
        assertTrue("seed-" + customName + " did not apply within 30s", applied.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("seed-" + customName + " failed", failure.get());
        }
    }

    public void testEachInstallOrRotationAddsExactlyOneNewKey() throws Exception {
        // Wait for two install or rotation events
        assertBusy(() -> assertThat(keyAddEvents, hasSize(greaterThanOrEqualTo(2))), 30, TimeUnit.SECONDS);
        for (int count : keyAddEvents) {
            assertThat("each install or rotation must add exactly one key", count, equalTo(1));
        }
        assertThat("no handlers registered, handlerKeyIds must be empty", metadataOnMaster().getHandlerKeyIds(), anEmptyMap());
    }

    public void testRotationCycleWithNoHandlers() throws Exception {
        String originalKeyId = metadataOnMaster().getActiveKeyId();
        // With no handlers registered, ticks rotate the active key on each rotation_interval and retire any non-active key
        // older than the grace window (10 * check_interval). The original install key eventually ages past grace and is dropped.
        assertBusy(() -> {
            ProjectEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m, notNullValue());
            assertThat("active key id should rotate", m.getActiveKeyId(), not(equalTo(originalKeyId)));
        }, 30, TimeUnit.SECONDS);
        assertBusy(() -> {
            ProjectEncryptionKeyMetadata m = metadataOnMaster();
            assertThat("the original install key should eventually be retired", m.getKeys().keySet(), not(hasItem(originalKeyId)));
        }, 60, TimeUnit.SECONDS);
    }

    public void testRotationCompletesWithMultipleHandlers() throws Exception {
        TestControlState alpha = registerHandlerOnAllNodes(AlphaBlob.TYPE, AlphaBlob::new);
        TestControlState beta = registerHandlerOnAllNodes(BetaBlob.TYPE, BetaBlob::new);

        EncryptionService svc = masterEncryptionService();
        EncryptedData alphaInitial = svc.encrypt("alex-data".getBytes(StandardCharsets.UTF_8));
        EncryptedData betaInitial = svc.encrypt("limelight-data".getBytes(StandardCharsets.UTF_8));
        seedBlob(AlphaBlob.TYPE, new AlphaBlob(alphaInitial));
        seedBlob(BetaBlob.TYPE, new BetaBlob(betaInitial));
        String initialKeyId = metadataOnMaster().getActiveKeyId();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata m = metadataOnMaster();
            assertThat("active key should have rotated", m.getActiveKeyId(), not(equalTo(initialKeyId)));
            assertThat("alpha handler invoked", alpha.callCount.get(), greaterThanOrEqualTo(1));
            assertThat("beta handler invoked", beta.callCount.get(), greaterThanOrEqualTo(1));
            AlphaBlob alphaBlob = customOnMaster(AlphaBlob.TYPE);
            BetaBlob betaBlob = customOnMaster(BetaBlob.TYPE);
            assertThat("alpha blob re-encrypted off initial", alphaBlob.blob.keyId(), not(equalTo(initialKeyId)));
            assertThat("beta blob re-encrypted off initial", betaBlob.blob.keyId(), not(equalTo(initialKeyId)));
            assertThat("alpha blob's key still present", m.getKeys().keySet(), hasItem(alphaBlob.blob.keyId()));
            assertThat("beta blob's key still present", m.getKeys().keySet(), hasItem(betaBlob.blob.keyId()));
        }, 30, TimeUnit.SECONDS);

        AlphaBlob alphaBlob = customOnMaster(AlphaBlob.TYPE);
        BetaBlob betaBlob = customOnMaster(BetaBlob.TYPE);
        assertEquals("alex-data", new String(masterEncryptionService().decrypt(alphaBlob.blob), StandardCharsets.UTF_8));
        assertEquals("limelight-data", new String(masterEncryptionService().decrypt(betaBlob.blob), StandardCharsets.UTF_8));
    }

    public void testConsecutiveRotations() throws Exception {
        TestControlState state = registerHandlerOnAllNodes(AlphaBlob.TYPE, AlphaBlob::new);
        seedBlob(AlphaBlob.TYPE, new AlphaBlob(masterEncryptionService().encrypt("freewill".getBytes(StandardCharsets.UTF_8))));
        String initialKeyId = metadataOnMaster().getActiveKeyId();

        AtomicReference<String> firstRotatedKeyId = new AtomicReference<>();
        assertBusy(() -> {
            String activeKeyId = metadataOnMaster().getActiveKeyId();
            assertThat(activeKeyId, not(equalTo(initialKeyId)));
            firstRotatedKeyId.set(activeKeyId);
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            String activeKeyId = metadataOnMaster().getActiveKeyId();
            assertThat("second rotation should advance active key id beyond initial", activeKeyId, not(equalTo(initialKeyId)));
            assertThat(
                "second rotation should advance active key id beyond first rotation",
                activeKeyId,
                not(equalTo(firstRotatedKeyId.get()))
            );
        }, 30, TimeUnit.SECONDS);
        // Reference unused-but-meaningful counter so the test still depends on real handler invocations.
        assertThat("handler invoked across consecutive rotations", state.callCount.get(), greaterThanOrEqualTo(1));
    }

    public void testRotationContinuesAfterMasterFailover() throws Exception {
        TestControlState state = registerHandlerOnAllNodes(AlphaBlob.TYPE, AlphaBlob::new);
        EncryptedData seed = masterEncryptionService().encrypt("tom sawyer".getBytes(StandardCharsets.UTF_8));
        seedBlob(AlphaBlob.TYPE, new AlphaBlob(seed));
        String preRotationKeyId = seed.keyId();

        // Original master rotates at least once and commits the blob's new key id into cluster state.
        AtomicReference<String> firstObservedKeyId = new AtomicReference<>();
        assertBusy(() -> {
            AlphaBlob current = customOnMaster(AlphaBlob.TYPE);
            assertThat("original master re-encrypted off the install key", current.blob.keyId(), not(equalTo(preRotationKeyId)));
            assertThat("original master invoked the handler", state.callCount.get(), greaterThanOrEqualTo(1));
            firstObservedKeyId.set(current.blob.keyId());
        }, 30, TimeUnit.SECONDS);

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        // The new master's coordinator must continue rotation on its first tick: the blob's key id in cluster state advances
        // beyond what the original master committed, and that newer key is still present in metadata (no orphan).
        assertBusy(() -> {
            AlphaBlob current = customOnMaster(AlphaBlob.TYPE);
            assertThat("new master advanced the blob's key id", current.blob.keyId(), not(equalTo(firstObservedKeyId.get())));
            assertThat("blob's key still present in metadata", metadataOnMaster().getKeys().keySet(), hasItem(current.blob.keyId()));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test plugin contributed via {@link #nodePlugins()} purely to register the {@link Metadata.ProjectCustom} subtypes used by the
     * handlers below.
     */
    public static class TestEncryptionCustomsPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, AlphaBlob.TYPE, AlphaBlob::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, AlphaBlob.TYPE, AlphaBlob::readDiffFrom),
                new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, BetaBlob.TYPE, BetaBlob::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, BetaBlob.TYPE, BetaBlob::readDiffFrom)
            );
        }
    }

    abstract static class TestBlobBase extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        final EncryptedData blob;

        TestBlobBase(EncryptedData blob) {
            this.blob = Objects.requireNonNull(blob);
        }

        TestBlobBase(StreamInput in) throws IOException {
            this.blob = new EncryptedData(in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            blob.writeTo(out);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("blob", blob));
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == getClass() && ((TestBlobBase) o).blob.equals(blob);
        }

        @Override
        public int hashCode() {
            return blob.hashCode();
        }
    }

    public static final class AlphaBlob extends TestBlobBase {
        static final String TYPE = "keyrotation_it_alpha";

        public AlphaBlob(EncryptedData blob) {
            super(blob);
        }

        public AlphaBlob(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }
    }

    public static final class BetaBlob extends TestBlobBase {
        static final String TYPE = "keyrotation_it_beta";

        public BetaBlob(EncryptedData blob) {
            super(blob);
        }

        public BetaBlob(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
        }
    }

    /**
     * Shared in-JVM state read by the test thread.
     */
    private static final class TestControlState {
        final AtomicInteger callCount = new AtomicInteger();
    }

    /**
     * Re-encrypts a {@link TestBlobBase}-shaped custom under the active key.
     */
    private record TestHandler<T extends TestBlobBase>(
        String customName,
        Function<EncryptedData, T> factory,
        EncryptionService encryptionService,
        TestControlState state
    ) implements EncryptedDataHandler<T> {

        @Override
        public T reEncrypt(T current, EncryptionService service, String activeKeyId) {
            state.callCount.incrementAndGet();
            if (current == null) {
                return null;
            }
            if (current.blob.keyId().equals(activeKeyId)) {
                return current;
            }
            byte[] plaintext = encryptionService.decrypt(current.blob);
            return factory.apply(encryptionService.encrypt(plaintext));
        }
    }
}
