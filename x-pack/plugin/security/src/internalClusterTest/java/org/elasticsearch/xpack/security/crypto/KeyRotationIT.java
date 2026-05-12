/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;
import org.elasticsearch.xpack.core.crypto.KeyRotationHandler;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata.RotationState;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class KeyRotationIT extends SecurityIntegTestCase {

    @Before
    public void checkFeatureFlag() {
        assumeTrue(
            "primary encryption key feature flag must be enabled",
            PrimaryEncryptionKeyService.PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(PrimaryEncryptionKeyService.ROTATION_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(PrimaryEncryptionKeyService.CHECK_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    @Before
    public void waitForPrimaryEncryptionKeyInstalled() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(metadataOnMaster(), notNullValue()));
    }

    private PrimaryEncryptionKeyMetadata metadataOnMaster() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return clusterService.state().metadata().getSingleProjectCustom(PrimaryEncryptionKeyMetadata.TYPE);
    }

    private EncryptionService masterEncryptionService() {
        return internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
    }

    /**
     * Registers a {@link TestHandler} on every node, sharing a single {@link TestControlState} across them, so the
     * handler that runs on whichever node is currently master observes/mutates the same state from the test thread.
     */
    private TestControlState registerHandlerOnAllNodes(String name) {
        final TestControlState state = new TestControlState();
        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService service = internalCluster().getInstance(EncryptionService.class, nodeName);
            service.registerKeyRotationHandler(new TestHandler(name, service, state));
        }
        return state;
    }

    private void seedBlob(TestControlState state, String plaintext) {
        EncryptedData encrypted = masterEncryptionService().encrypt(plaintext.getBytes(StandardCharsets.UTF_8));
        state.blob.set(encrypted);
    }

    public void testRotationCycleWithNoHandlers() throws Exception {
        String originalKeyId = metadataOnMaster().getActiveKeyId();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m, notNullValue());
            assertThat("active key id should rotate", m.getActiveKeyId(), not(equalTo(originalKeyId)));
            assertThat("rotation should complete", m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat("only the active key should remain after retire", m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
        }, 30, TimeUnit.SECONDS);
    }

    public void testHandlerIsInvokedAndDataIsReEncrypted() throws Exception {
        TestControlState state = registerHandlerOnAllNodes("peart");
        seedBlob(state, "2112");
        String initialKeyId = state.blob.get().keyId();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m, notNullValue());
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat("only the active key should remain", m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
            assertThat("blob should be re-encrypted under the new key", state.blob.get().keyId(), not(equalTo(initialKeyId)));
            assertThat(
                "handler observed the active key id matching metadata",
                state.observedActiveKeyId.get(),
                equalTo(m.getActiveKeyId())
            );
        }, 30, TimeUnit.SECONDS);

        byte[] decrypted = masterEncryptionService().decrypt(state.blob.get());
        assertEquals("2112", new String(decrypted, StandardCharsets.UTF_8));
    }

    public void testRotationStallsWhenHandlerReportsFailure() throws Exception {
        TestControlState state = registerHandlerOnAllNodes("geddy");
        state.failMode = TestFailModeTest.ON_FAILURE;
        seedBlob(state, "moving pictures");

        // A failing handler is invoked at least twice (proving the coordinator retries) and rotation does not retire.
        assertBusy(() -> {
            assertThat(metadataOnMaster().getRotationState(), equalTo(RotationState.ROTATING));
            assertThat(state.callCount.get(), greaterThanOrEqualTo(2));
        }, 30, TimeUnit.SECONDS);
        // Old key is still present because retire is gated on handler success.
        assertThat(metadataOnMaster().getKeys().size(), greaterThanOrEqualTo(2));

        state.failMode = TestFailModeTest.OK;
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat(m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
            assertThat(state.blob.get().keyId(), equalTo(m.getActiveKeyId()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testRotationContinuesWhenHandlerThrowsSynchronously() throws Exception {
        TestControlState state = registerHandlerOnAllNodes("lifeson");
        state.failMode = TestFailModeTest.THROW;
        seedBlob(state, "clockwork angels");

        assertBusy(() -> {
            assertThat(metadataOnMaster().getRotationState(), equalTo(RotationState.ROTATING));
            // Keep failing on every tick, should reach call count 2 after a couple of seconds
            assertThat(state.callCount.get(), greaterThanOrEqualTo(2));
        }, 30, TimeUnit.SECONDS);

        state.failMode = TestFailModeTest.OK;
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat(m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
            assertThat(state.blob.get().keyId(), equalTo(m.getActiveKeyId()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testRotationCompletesWithMultipleHandlers() throws Exception {
        TestControlState controlState1 = registerHandlerOnAllNodes("alex");
        TestControlState controlState2 = registerHandlerOnAllNodes("limelight");
        seedBlob(controlState1, "alex-data");
        seedBlob(controlState2, "limelight-data");
        String initialKeyId = metadataOnMaster().getActiveKeyId();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat("active key should have rotated", m.getActiveKeyId(), not(equalTo(initialKeyId)));
            assertThat("the previous active key should be retired", m.getKeys().keySet(), not(hasItem(initialKeyId)));
            assertThat("only the new active key should remain", m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
            assertThat("controlState1 was invoked", controlState1.callCount.get(), greaterThanOrEqualTo(1));
            assertThat("controlState2 was invoked", controlState2.callCount.get(), greaterThanOrEqualTo(1));
            assertThat(controlState1.blob.get().keyId(), equalTo(m.getActiveKeyId()));
            assertThat(controlState2.blob.get().keyId(), equalTo(m.getActiveKeyId()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testMultipleHandlersOneFailureStallsRotation() throws Exception {
        TestControlState controlState1 = registerHandlerOnAllNodes("neil");
        TestControlState controlState2 = registerHandlerOnAllNodes("lee");
        controlState2.failMode = TestFailModeTest.ON_FAILURE;
        seedBlob(controlState1, "neil-data");
        seedBlob(controlState2, "lee-data");

        assertBusy(() -> {
            assertThat(metadataOnMaster().getRotationState(), equalTo(RotationState.ROTATING));
            assertThat(controlState2.callCount.get(), greaterThanOrEqualTo(2));
        }, 30, TimeUnit.SECONDS);
        assertThat(metadataOnMaster().getKeys().size(), equalTo(2));

        controlState2.failMode = TestFailModeTest.OK;
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat(m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
            assertThat(controlState1.blob.get().keyId(), equalTo(m.getActiveKeyId()));
            assertThat(controlState2.blob.get().keyId(), equalTo(m.getActiveKeyId()));
            assertThat(metadataOnMaster().getKeys().size(), equalTo(1));
        }, 30, TimeUnit.SECONDS);
    }

    public void testConsecutiveRotations() throws Exception {
        TestControlState state = registerHandlerOnAllNodes("pert");
        seedBlob(state, "freewill");
        String initialKeyId = metadataOnMaster().getActiveKeyId();

        AtomicReference<String> firstRotatedKeyId = new AtomicReference<>();
        assertBusy(() -> {
            String activeKeyId = metadataOnMaster().getActiveKeyId();
            assertThat(activeKeyId, not(equalTo(initialKeyId)));
            firstRotatedKeyId.set(activeKeyId);
        }, 30, TimeUnit.SECONDS);

        // A second rotation cycle must produce yet another distinct active key id.
        assertBusy(() -> {
            String activeKeyId = metadataOnMaster().getActiveKeyId();
            assertThat("second rotation should advance active key id beyond initial", activeKeyId, not(equalTo(initialKeyId)));
            assertThat(
                "second rotation should advance active key id beyond first rotation",
                activeKeyId,
                not(equalTo(firstRotatedKeyId.get()))
            );
        }, 30, TimeUnit.SECONDS);
    }

    public void testRotationContinuesAfterMasterFailoverMidRotation() throws Exception {
        TestControlState state = registerHandlerOnAllNodes("midflight");
        state.blockUntilReleased = true;
        seedBlob(state, "tom sawyer");
        String preRotationKeyId = state.blob.get().keyId();

        // Wait for rotation to begin and the original master's handler to be invoked at least once
        // (it is now blocked on the release latch, pinning the cluster in ROTATING).
        assertBusy(() -> {
            assertThat(metadataOnMaster().getRotationState(), equalTo(RotationState.ROTATING));
            assertThat(state.callCount.get(), greaterThanOrEqualTo(1));
        }, 30, TimeUnit.SECONDS);

        // The mid-rotation active key id was chosen by the original master's BeginRotation. The cycle
        // must complete with this same key id even after the master changes.
        String midRotationKeyId = metadataOnMaster().getActiveKeyId();
        assertThat(midRotationKeyId, not(equalTo(preRotationKeyId)));

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        // The new master's coordinator picks up the ROTATING state and invokes the handler on its first tick.
        assertBusy(() -> assertThat(state.callCount.get(), greaterThanOrEqualTo(2)), 30, TimeUnit.SECONDS);
        // Cluster state is still ROTATING — the (now new) handler invocation is blocked on the latch.
        assertThat(metadataOnMaster().getRotationState(), equalTo(RotationState.ROTATING));
        assertThat(metadataOnMaster().getActiveKeyId(), equalTo(midRotationKeyId));

        // Release the handler. Rotation retires and returns to STABLE retaining the mid-rotation key.
        state.releaseLatch.countDown();
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m.getRotationState(), equalTo(RotationState.STABLE));
            assertThat(m.getActiveKeyId(), equalTo(midRotationKeyId));
            assertThat(m.getKeys().keySet(), equalTo(Set.of(m.getActiveKeyId())));
        }, 30, TimeUnit.SECONDS);
    }

    private enum TestFailModeTest {
        OK,
        ON_FAILURE,
        THROW
    }

    private static final class TestControlState {
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicReference<EncryptedData> blob = new AtomicReference<>();
        final AtomicReference<String> observedActiveKeyId = new AtomicReference<>();
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        volatile TestFailModeTest failMode = TestFailModeTest.OK;
        // When true, the OK arm of the handler awaits releaseLatch before completing the listener.
        // Used by the master-failover test to pin the cluster in ROTATING.
        volatile boolean blockUntilReleased = false;
    }

    private record TestHandler(String name, EncryptionService service, TestControlState state) implements KeyRotationHandler {

        @Override
        public void reEncrypt(String activeKeyId, ActionListener<Void> listener) {
            state.callCount.incrementAndGet();
            state.observedActiveKeyId.set(activeKeyId);
            switch (state.failMode) {
                case THROW -> throw new RuntimeException("simulated synchronous throw from handler [" + name + "]");
                case ON_FAILURE -> listener.onFailure(new RuntimeException("simulated failure from handler [" + name + "]"));
                case OK -> {
                    EncryptedData current = state.blob.get();
                    if (current != null && current.keyId().equals(activeKeyId) == false) {
                        byte[] plaintext = service.decrypt(current);
                        state.blob.set(service.encrypt(plaintext));
                    }
                    if (state.blockUntilReleased) {
                        try {
                            if (state.releaseLatch.await(60, TimeUnit.SECONDS) == false) {
                                listener.onFailure(new RuntimeException("release latch timed out"));
                                return;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            listener.onFailure(e);
                            return;
                        }
                    }
                    listener.onResponse(null);
                }
            }
        }
    }
}
