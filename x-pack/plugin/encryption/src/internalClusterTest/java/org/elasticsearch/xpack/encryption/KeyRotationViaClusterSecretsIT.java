/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.stateless.settings.secure.SecureSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end coverage of the stateless password-rotation trigger path. Where {@link KeyRotationIT} exercises rotation by
 * driving the coordinator directly:
 *
 * <ol>
 *   <li>{@link FileSettingsService} watches the operator file directory.</li>
 *   <li>A new JSON is dropped containing a {@code cluster_secrets} block carrying the encryption-related password material.</li>
 *   <li>The {@code cluster_secrets} reserved-state handler writes a new {@link ClusterSecrets} cluster-state custom.</li>
 *   <li>{@code ClusterStateSecretsListener} (installed by {@link SecureSettingsPlugin}) sees the version bump and calls
 *       {@code reloadCallback.reload(...)} on every node, which fans out to every {@link org.elasticsearch.plugins.ReloadablePlugin}
 *       — notably {@link EncryptionPlugin}, which refreshes its in-memory password snapshot.</li>
 *   <li>{@link KeyRotationCoordinator} sees the {@code activePasswordId} mismatch with the persisted PEK metadata, rewraps the
 *       PEK under the new password, and persists the result as a new {@link ProjectEncryptionKeyMetadata}.</li>
 * </ol>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class KeyRotationViaClusterSecretsIT extends ESIntegTestCase {

    private static final String INITIAL_PASSWORD_ID = "v1";
    private static final String INITIAL_PASSWORD = "encryption-test-password";
    private static final String ROTATED_PASSWORD_ID = "v2";
    private static final String ROTATED_PASSWORD = "encryption-test-password-v2";

    private final AtomicLong fileSettingsVersionCounter = new AtomicLong(1);

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, INITIAL_PASSWORD_ID);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + INITIAL_PASSWORD_ID, INITIAL_PASSWORD);
        // Fast tick to catch any race conditions within the test timeout
        builder.put(KeyRotationCoordinator.CHECK_INTERVAL_SETTING.getKey(), "1s").setSecureSettings(secure);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(SecureSettingsPlugin.class);
        return plugins;
    }

    @Before
    public void waitForInitialPekInstall() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(metadataOnMaster(), notNullValue()));
    }

    public void testPasswordRotationViaClusterSecrets() throws Exception {
        String master = internalCluster().getMasterName();

        // Make sure the file-settings watcher is live before we drop the JSON
        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, master);
        assertBusy(() -> assertTrue("file settings service must be watching before we write the operator file", fss.watching()));

        // Encrypt a known plaintext under the v1 PEK
        EncryptedData blobUnderV1 = masterEncryptionService().encrypt("password-rotation-payload".getBytes(StandardCharsets.UTF_8));

        ProjectEncryptionKeyMetadata installMetadata = metadataOnMaster();
        String installKeyId = installMetadata.getActiveKeyId();
        byte[] installPlaintextBytes = installMetadata.getKeys().get(installKeyId).bytes().clone();
        assertThat(installMetadata.getPasswordId(), equalTo(INITIAL_PASSWORD_ID));

        // Publish v1+v2 + active=v2 via the operator file-settings JSON. v1 must remain available so the rewrap can unwrap the
        // existing PEK before wrapping it under v2
        long version = fileSettingsVersionCounter.incrementAndGet();
        CountDownLatch applied = fileSettingsAppliedLatch(master, version);
        writeFileSettingsJson(master, encryptionPasswordsJson(version, INITIAL_PASSWORD, ROTATED_PASSWORD, ROTATED_PASSWORD_ID));
        assertTrue("file-settings update was not applied within 30s", applied.await(30, TimeUnit.SECONDS));

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata m = metadataOnMaster();
            assertThat("metadata should record the new active password id", m.getPasswordId(), equalTo(ROTATED_PASSWORD_ID));
            ProjectEncryptionKeyMetadata.KeyEntry installKeyAfter = m.getKeys().get(installKeyId);
            assertThat("install key must still be present after password rotation", installKeyAfter, notNullValue());
            assertTrue(
                "plaintext key bytes must be preserved across password rotation",
                Arrays.equals(installPlaintextBytes, installKeyAfter.bytes())
            );
        }, 30, TimeUnit.SECONDS);

        // Plaintext PEK must be preserved across the rewrap
        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertEquals(
                "node [" + nodeName + "] must decrypt the pre-rotation blob after password rotation",
                "password-rotation-payload",
                new String(svc.decrypt(blobUnderV1), StandardCharsets.UTF_8)
            );
        }

        // GET _cluster/state/metadata is the canonical channel to learn that the password rotation has completed and the previous
        // password can be removed from the operator secret store.
        Map<String, Object> pekFromRest = projectEncryptionKeyFromClusterStateApi();
        assertThat(
            "password_id must advance to v2 on the public REST surface",
            pekFromRest.get("password_id"),
            equalTo(ROTATED_PASSWORD_ID)
        );
        assertThat("active_key_id must be present", pekFromRest.get("active_key_id"), notNullValue());
        assertThat("API context must redact per-key wrapped bytes", findKeyEntryField(pekFromRest, "bytes"), nullValue());
        assertThat("API context must keep per-key generated_at", findKeyEntryField(pekFromRest, "generated_at"), notNullValue());

        // Defensive raw-JSON check: nothing under the encryption custom should serialize a "bytes" field
        String rawJson = rawClusterStateMetadataJson();
        int customStart = rawJson.indexOf("\"" + ProjectEncryptionKeyMetadata.TYPE + "\":{");
        assertThat("expected the encryption custom to appear in raw cluster state JSON", customStart, not(equalTo(-1)));
        String customSlice = rawJson.substring(customStart, Math.min(rawJson.length(), customStart + 2048));
        assertThat("raw JSON for the encryption custom must not contain a 'bytes' field", customSlice, not(containsString("\"bytes\"")));
    }

    public void testNewEncryptionAfterRotation() throws Exception {
        String master = internalCluster().getMasterName();
        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, master);
        assertBusy(() -> assertTrue(fss.watching()));

        long version = fileSettingsVersionCounter.incrementAndGet();
        CountDownLatch applied = fileSettingsAppliedLatch(master, version);
        writeFileSettingsJson(master, encryptionPasswordsJson(version, INITIAL_PASSWORD, ROTATED_PASSWORD, ROTATED_PASSWORD_ID));
        assertTrue("file-settings update was not applied within 30s", applied.await(30, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(metadataOnMaster().getPasswordId(), equalTo(ROTATED_PASSWORD_ID)), 30, TimeUnit.SECONDS);

        // Data encrypted after the rotation must be decryptable on every node
        EncryptedData postRotationBlob = masterEncryptionService().encrypt("post-rotation-payload".getBytes(StandardCharsets.UTF_8));
        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertEquals(
                "node [" + nodeName + "] must decrypt post-rotation blob",
                "post-rotation-payload",
                new String(svc.decrypt(postRotationBlob), StandardCharsets.UTF_8)
            );
        }
    }

    public void testOldPasswordRemovableAfterRotation() throws Exception {
        String master = internalCluster().getMasterName();
        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, master);
        assertBusy(() -> assertTrue(fss.watching()));

        // Encrypt before rotation so we can verify it's still readable afterwards
        EncryptedData preRotationBlob = masterEncryptionService().encrypt("pre-rotation-payload".getBytes(StandardCharsets.UTF_8));

        // Rotate to v2 (both passwords present so the rewrap can read the old wrapped bytes)
        long version1 = fileSettingsVersionCounter.incrementAndGet();
        CountDownLatch applied1 = fileSettingsAppliedLatch(master, version1);
        writeFileSettingsJson(master, encryptionPasswordsJson(version1, INITIAL_PASSWORD, ROTATED_PASSWORD, ROTATED_PASSWORD_ID));
        assertTrue("rotation file-settings update was not applied within 30s", applied1.await(30, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(metadataOnMaster().getPasswordId(), equalTo(ROTATED_PASSWORD_ID)), 30, TimeUnit.SECONDS);

        EncryptedData postRotationBlob = masterEncryptionService().encrypt("post-rotation-payload".getBytes(StandardCharsets.UTF_8));

        // Drop v1 — only v2 remains
        long version2 = fileSettingsVersionCounter.incrementAndGet();
        CountDownLatch applied2 = fileSettingsAppliedLatch(master, version2);
        writeFileSettingsJson(master, rotatedPasswordOnlyJson(version2));
        assertTrue("v1-removal file-settings update was not applied within 30s", applied2.await(30, TimeUnit.SECONDS));

        // Pre- and post-rotation blobs must still be decryptable on every node after v1 is removed
        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertEquals(
                "node [" + nodeName + "] must decrypt pre-rotation blob after v1 password removal",
                "pre-rotation-payload",
                new String(svc.decrypt(preRotationBlob), StandardCharsets.UTF_8)
            );
            assertEquals(
                "node [" + nodeName + "] must decrypt post-rotation blob after v1 password removal",
                "post-rotation-payload",
                new String(svc.decrypt(postRotationBlob), StandardCharsets.UTF_8)
            );
        }
    }

    private Map<String, Object> projectEncryptionKeyFromClusterStateApi() throws IOException {
        ObjectPath path = ObjectPath.createFromResponse(performClusterStateMetadataRequest());
        Map<String, Object> pek = path.evaluate("metadata." + ProjectEncryptionKeyMetadata.TYPE);
        assertThat("expected " + ProjectEncryptionKeyMetadata.TYPE + " in cluster state metadata", pek, notNullValue());
        return pek;
    }

    private String rawClusterStateMetadataJson() throws IOException {
        Response response = performClusterStateMetadataRequest();
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static Response performClusterStateMetadataRequest() throws IOException {
        return getRestClient().performRequest(new Request("GET", "/_cluster/state/metadata"));
    }

    @SuppressWarnings("unchecked")
    private static Object findKeyEntryField(Map<String, Object> pek, String fieldName) {
        Map<String, Object> keys = (Map<String, Object>) pek.get("keys");
        if (keys == null || keys.isEmpty()) {
            return null;
        }
        Map<String, Object> firstEntry = (Map<String, Object>) keys.values().iterator().next();
        return firstEntry.get(fieldName);
    }

    private ProjectEncryptionKeyMetadata metadataOnMaster() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private EncryptionService masterEncryptionService() {
        return internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
    }

    private static void writeFileSettingsJson(String node, String json) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, json);
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
    }

    private static CountDownLatch fileSettingsAppliedLatch(String node, long expectedVersion) {
        ClusterService cs = internalCluster().clusterService(node);
        CountDownLatch latch = new CountDownLatch(1);
        ClusterStateListener[] selfRef = new ClusterStateListener[1];
        selfRef[0] = event -> {
            ReservedStateMetadata rsm = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
            if (rsm != null && rsm.version() == expectedVersion && rsm.handlers().get(ClusterSecrets.NAME) != null) {
                cs.removeListener(selfRef[0]);
                latch.countDown();
            }
        };
        cs.addListener(selfRef[0]);
        return latch;
    }

    private static String rotatedPasswordOnlyJson(long version) {
        return Strings.format(
            """
                {
                    "metadata": {
                        "version": "%s",
                        "compatibility": "8.4.0"
                    },
                    "state": {
                        "cluster_secrets": {
                            "string_secrets": {
                                "%s": "%s",
                                "%s": "%s"
                            }
                        }
                    }
                }""",
            version,
            ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + ROTATED_PASSWORD_ID,
            ROTATED_PASSWORD,
            ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
            ROTATED_PASSWORD_ID
        );
    }

    private static String encryptionPasswordsJson(long version, String v1Password, String v2Password, String activePasswordId) {
        return Strings.format(
            """
                {
                    "metadata": {
                        "version": "%s",
                        "compatibility": "8.4.0"
                    },
                    "state": {
                        "cluster_secrets": {
                            "string_secrets": {
                                "%s": "%s",
                                "%s": "%s",
                                "%s": "%s"
                            }
                        }
                    }
                }""",
            version,
            ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + INITIAL_PASSWORD_ID,
            v1Password,
            ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + ROTATED_PASSWORD_ID,
            v2Password,
            ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
            activePasswordId
        );
    }
}
