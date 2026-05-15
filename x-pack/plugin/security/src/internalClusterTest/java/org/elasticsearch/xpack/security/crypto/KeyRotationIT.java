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
import org.elasticsearch.encryption.EncryptedDataHandler;
import org.elasticsearch.encryption.EncryptedDataHandlerRegistry;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
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
            .put(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
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
     * Registers a {@link TestHandler} on every node, sharing a single {@link TestControlState} across them, so the handler that runs on
     * whichever node is currently master observes/mutates the same state from the test thread.
     */
    private TestControlState registerHandlerOnAllNodes() {
        final TestControlState state = new TestControlState();
        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService service = internalCluster().getInstance(EncryptionService.class, nodeName);
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
            EncryptedDataHandlerRegistry registry = internalCluster().getInstance(EncryptedDataHandlerRegistry.class, nodeName);
            registry.register(new TestHandler(service, threadPool, state));
        }
        return state;
    }

    private void seedBlob(TestControlState state, String plaintext) {
        EncryptedData encrypted = masterEncryptionService().encrypt(plaintext.getBytes(StandardCharsets.UTF_8));
        state.blob.set(encrypted);
    }

    public void testRotationCycleWithNoHandlers() throws Exception {
        String originalKeyId = metadataOnMaster().getActiveKeyId();
        // With no handlers registered, ticks rotate the active key on each rotation_interval and retire any non-active key
        // older than the grace window (10 * check_interval). The original install key eventually ages past grace and is dropped.
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat(m, notNullValue());
            assertThat("active key id should rotate", m.getActiveKeyId(), not(equalTo(originalKeyId)));
        }, 30, TimeUnit.SECONDS);
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat("the original install key should eventually be retired", m.getKeys().keySet(), not(hasItem(originalKeyId)));
        }, 60, TimeUnit.SECONDS);
    }

    public void testRotationCompletesWithMultipleHandlers() throws Exception {
        TestControlState alpha = registerHandlerOnAllNodes();
        TestControlState beta = registerHandlerOnAllNodes();
        seedBlob(alpha, "alex-data");
        seedBlob(beta, "limelight-data");
        String initialKeyId = metadataOnMaster().getActiveKeyId();

        // Both handlers' data eventually moves off the initial key and onto a still-live key in metadata.
        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata m = metadataOnMaster();
            assertThat("active key should have rotated", m.getActiveKeyId(), not(equalTo(initialKeyId)));
            assertThat("alpha handler invoked", alpha.callCount.get(), greaterThanOrEqualTo(1));
            assertThat("beta handler invoked", beta.callCount.get(), greaterThanOrEqualTo(1));
            assertThat("alpha blob re-encrypted off initial", alpha.blob.get().keyId(), not(equalTo(initialKeyId)));
            assertThat("beta blob re-encrypted off initial", beta.blob.get().keyId(), not(equalTo(initialKeyId)));
            assertThat("alpha blob key still present", m.getKeys().keySet(), hasItem(alpha.blob.get().keyId()));
            assertThat("beta blob key still present", m.getKeys().keySet(), hasItem(beta.blob.get().keyId()));
        }, 30, TimeUnit.SECONDS);

        // End-to-end round-trip: re-encrypted blobs still decrypt back to the original plaintext.
        assertEquals("alex-data", new String(masterEncryptionService().decrypt(alpha.blob.get()), StandardCharsets.UTF_8));
        assertEquals("limelight-data", new String(masterEncryptionService().decrypt(beta.blob.get()), StandardCharsets.UTF_8));
    }

    public void testConsecutiveRotations() throws Exception {
        // Rotation cadence honors rotation_interval exactly; the grace window for retire does NOT block the next rotation.
        TestControlState state = registerHandlerOnAllNodes();
        seedBlob(state, "freewill");
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
    }

    public void testRotationContinuesAfterMasterFailoverMidRotation() throws Exception {
        TestControlState state = registerHandlerOnAllNodes();
        state.blockUntilReleased = true;
        seedBlob(state, "tom sawyer");
        String preRotationKeyId = state.blob.get().keyId();

        // Wait for rotation to begin and the original master's handler to be invoked at least once (it is now blocked on the latch).
        assertBusy(() -> {
            assertThat("multiple keys should be present mid-rotation", metadataOnMaster().getKeys().size(), greaterThanOrEqualTo(2));
            assertThat(state.callCount.get(), greaterThanOrEqualTo(1));
        }, 30, TimeUnit.SECONDS);

        String midRotationKeyId = metadataOnMaster().getActiveKeyId();
        assertThat(midRotationKeyId, not(equalTo(preRotationKeyId)));

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        // The new master's coordinator continues the scrub on its first tick.
        assertBusy(() -> assertThat(state.callCount.get(), greaterThanOrEqualTo(2)), 30, TimeUnit.SECONDS);

        // Release the handler. Eventually the blob is re-encrypted off the original pre-rotation key onto some live key.
        state.releaseLatch.countDown();
        assertBusy(() -> {
            assertThat("blob re-encrypted off pre-rotation key", state.blob.get().keyId(), not(equalTo(preRotationKeyId)));
            assertThat("blob's key still present in metadata", metadataOnMaster().getKeys().keySet(), hasItem(state.blob.get().keyId()));
        }, 30, TimeUnit.SECONDS);
    }

    private enum TestFailMode {
        OK,
        ON_FAILURE,
        THROW
    }

    private static final class TestControlState {
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicReference<EncryptedData> blob = new AtomicReference<>();
        final AtomicReference<String> observedActiveKeyId = new AtomicReference<>();
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        volatile TestFailMode failMode = TestFailMode.OK;
        // When true, the OK arm of the handler awaits releaseLatch before completing the listener.
        volatile boolean blockUntilReleased = false;
    }

    private record TestHandler(EncryptionService service, ThreadPool threadPool, TestControlState state) implements EncryptedDataHandler {

        @Override
        public void reEncrypt(String activeKeyId, ActionListener<Void> listener) {
            state.callCount.incrementAndGet();
            state.observedActiveKeyId.set(activeKeyId);
            switch (state.failMode) {
                case THROW -> throw new RuntimeException("simulated synchronous throw from test handler");
                case ON_FAILURE -> listener.onFailure(new RuntimeException("simulated failure from test handler"));
                case OK -> {
                    EncryptedData current = state.blob.get();
                    if (current != null && current.keyId().equals(activeKeyId) == false) {
                        byte[] plaintext = service.decrypt(current);
                        state.blob.set(service.encrypt(plaintext));
                    }
                    if (state.blockUntilReleased) {
                        // Complete the listener async, after the latch is released
                        threadPool.generic().execute(() -> {
                            try {
                                if (state.releaseLatch.await(60, TimeUnit.SECONDS) == false) {
                                    listener.onFailure(new RuntimeException("release latch timed out"));
                                } else {
                                    listener.onResponse(null);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                listener.onFailure(e);
                            }
                        });
                    } else {
                        listener.onResponse(null);
                    }
                }
            }
        }
    }
}
