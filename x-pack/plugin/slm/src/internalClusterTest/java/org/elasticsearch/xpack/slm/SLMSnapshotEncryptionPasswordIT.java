/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.encryption.EncryptingSnapshotGlobalStateTransformer;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.encryption.EncryptionResetRequest;
import org.elasticsearch.xpack.encryption.TransportEncryptionResetAction;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end test of the snapshot {@code encrypted_data} feature through the SLM production path: a policy's
 * PEK-encrypted password is re-wrapped under a snapshot-password-derived key when the policy executes, and re-wrapped
 * under the (destination) PEK when the snapshot is restored with the password. Exercises the whole chain: the
 * {@code SnapshotGlobalStateTransformer} SPI discovery, the transformer implementation in the encryption plugin, and
 * the SLM {@code EncryptedDataHandler} re-encrypt hooks.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, supportsDedicatedMasters = false)
public class SLMSnapshotEncryptionPasswordIT extends AbstractSnapshotIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PEK_PASSWORD = "pek-test-password";
    // Doubles as the policy's stored secret and the snapshot password when the policy executes (min length 15).
    private static final String ENCRYPTED_DATA_PASSWORD = "snapshot-password-for-testing";
    private static final String ENCRYPTED_DATA_PASSWORD_ID = "snapshot-password-id";
    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";
    private static final String REPO = "encrypted-repo";
    private static final String POLICY_ID = "encrypted-policy";
    private static final String INDEX = "test-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, SnapshotLifecycle.class, EncryptionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED, false);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.state.encryption.active_password_id", PASSWORD_ID);
        secureSettings.setString("cluster.state.encryption.password." + PASSWORD_ID, PEK_PASSWORD);
        builder.setSecureSettings(secureSettings);
        return builder.build();
    }

    @After
    public void drainRunningSnapshotOperations() throws Exception {
        awaitNoMoreRunningOperations();
    }

    public void testPolicyPasswordSurvivesSnapshotAndRestore() throws Exception {
        ensureGreen();
        waitForEncryptionService();

        createRepository(REPO, "fs");
        indexRandomDocs(INDEX, 10);
        putPolicyWithEncryptionPassword();

        // The stored policy has the plaintext stripped from its config and the password encrypted under the PEK
        SnapshotLifecyclePolicy stored = storedPolicy();
        assertThat(stored.getConfig().get("encrypted_data"), nullValue());
        assertThat(stored.getEncryptedPassword(), notNullValue());
        assertThat(stored.getEncryptedPasswordId(), equalTo(ENCRYPTED_DATA_PASSWORD_ID));
        assertThat(
            stored.getEncryptedPassword().keyId().equals(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID),
            equalTo(false)
        );

        // GET _slm/policy returns the encrypted_data type and password id but never the password itself
        String getResponse = getPolicyApiResponse();
        assertThat(getResponse, containsString(ENCRYPTED_DATA_PASSWORD_ID));
        assertThat(getResponse, not(containsString(ENCRYPTED_DATA_PASSWORD)));
        assertThat(getResponse, containsString("\"encrypted_data\":{\"type\":\"password\""));
        assertThat(getResponse, not(containsString("\"password\":{")));

        final String snapshotName = executePolicy();
        final SnapshotInfo snapshotInfo = awaitSuccessfulSnapshot(snapshotName);
        assertTrue("snapshot must be flagged as containing encrypted data", snapshotInfo.hasEncryptedData());
        assertThat(snapshotInfo.encryptedDataType(), equalTo("password"));
        assertThat(snapshotInfo.encryptedDataPasswordId(), equalTo(ENCRYPTED_DATA_PASSWORD_ID));

        // Wipe the policy and index so the restored state provably comes from the snapshot
        deletePolicyAndIndex();

        // Restoring with the wrong password must fail before any state is applied
        ExecutionException wrongPassword = expectThrows(
            ExecutionException.class,
            () -> restore(snapshotName, "wrong-password-of-sufficient-length").get()
        );
        assertThat(storedPolicyMetadata(), nullValue());
        assertNotNull(wrongPassword.getCause());

        // Restoring with the right password re-encrypts the policy password under this cluster's PEK
        RestoreSnapshotResponse restore = restore(snapshotName, ENCRYPTED_DATA_PASSWORD).get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        SnapshotLifecyclePolicy restored = storedPolicy();
        assertThat(restored.getEncryptedPassword(), notNullValue());
        assertThat(
            restored.getEncryptedPassword().keyId().equals(EncryptingSnapshotGlobalStateTransformer.SNAPSHOT_PASSWORD_KEY_ID),
            equalTo(false)
        );
        byte[] decrypted = masterEncryptionService().decrypt(restored.getEncryptedPassword());
        assertThat(new String(decrypted, StandardCharsets.UTF_8), equalTo(ENCRYPTED_DATA_PASSWORD));

        // Restoring without a password restores the policy but clears the unrecoverable password
        deletePolicyAndIndex();
        RestoreSnapshotResponse withoutPassword = restore(snapshotName, null).get();
        assertThat(withoutPassword.getRestoreInfo().failedShards(), equalTo(0));
        SnapshotLifecyclePolicy cleared = storedPolicy();
        assertThat(cleared.getEncryptedPassword(), nullValue());
    }

    public void testSnapshotWithoutPasswordExcludesPolicyPassword() throws Exception {
        ensureGreen();
        waitForEncryptionService();

        createRepository(REPO, "fs");
        indexRandomDocs(INDEX, 10);
        putPolicyWithEncryptionPassword();

        // A plain snapshot (no encrypted_data on the request) excludes the encrypted values from the
        // snapshot's global state entirely, so it never contains data wrapped under this cluster's PEK
        createSnapshot(REPO, "plain-snapshot", List.of(INDEX));
        assertFalse(
            "snapshot must not be flagged as containing password-protected data",
            getSnapshot(REPO, "plain-snapshot").hasEncryptedData()
        );

        deletePolicyAndIndex();

        // The policy restores, but its password was never in the snapshot
        RestoreSnapshotResponse restore = restore("plain-snapshot", null).get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        SnapshotLifecyclePolicy restored = storedPolicy();
        assertThat(restored.getEncryptedPassword(), nullValue());
    }

    public void testDestructiveResetClearsPolicyPasswordButPasswordProtectedSnapshotsRemainRestorable() throws Exception {
        ensureGreen();
        waitForEncryptionService();

        createRepository(REPO, "fs");
        indexRandomDocs(INDEX, 10);
        putPolicyWithEncryptionPassword();

        final String snapshotName = executePolicy();
        awaitSuccessfulSnapshot(snapshotName);

        // destroy the PEK; the SLM handler clears the policy password but keeps the policy
        client().execute(TransportEncryptionResetAction.TYPE, new EncryptionResetRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, true))
            .get();
        assertThat(storedPolicy().getEncryptedPassword(), nullValue());

        // a new PEK is installed automatically; the snapshot is password-wrapped and independent of any PEK,
        // so restoring with the password recovers the policy password under the new key
        waitForEncryptionService();
        deletePolicyAndIndex();
        RestoreSnapshotResponse restore = restore(snapshotName, ENCRYPTED_DATA_PASSWORD).get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        SnapshotLifecyclePolicy recovered = storedPolicy();
        assertThat(recovered.getEncryptedPassword(), notNullValue());
        byte[] decrypted = masterEncryptionService().decrypt(recovered.getEncryptedPassword());
        assertThat(new String(decrypted, StandardCharsets.UTF_8), equalTo(ENCRYPTED_DATA_PASSWORD));
    }

    private void waitForEncryptionService() throws Exception {
        assertBusy(() -> {
            try {
                byte[] roundTrip = masterEncryptionService().decrypt(masterEncryptionService().encrypt(new byte[] { 1 }));
                assertThat(roundTrip.length, equalTo(1));
            } catch (EncryptionKeyNotYetAvailableException e) {
                throw new AssertionError("project encryption key not yet installed", e);
            }
        }, 30, TimeUnit.SECONDS);
    }

    private EncryptionService masterEncryptionService() {
        return internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
    }

    private void putPolicyWithEncryptionPassword() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("indices", List.of(INDEX));
        config.put("include_global_state", true);
        config.put(
            "encrypted_data",
            Map.of("type", "password", "password", ENCRYPTED_DATA_PASSWORD, "password_id", ENCRYPTED_DATA_PASSWORD_ID)
        );
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(POLICY_ID, "snap", NEVER_EXECUTE_CRON_SCHEDULE, REPO, config, null);
        AcknowledgedResponse response = client().execute(
            PutSnapshotLifecycleAction.INSTANCE,
            new PutSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, POLICY_ID, policy)
        ).get();
        assertAcked(response);
    }

    private String executePolicy() throws Exception {
        return client().execute(
            ExecuteSnapshotLifecycleAction.INSTANCE,
            new ExecuteSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, POLICY_ID)
        ).get().getSnapshotName();
    }

    private SnapshotInfo awaitSuccessfulSnapshot(String snapshotName) throws Exception {
        assertBusy(() -> {
            final SnapshotInfo info;
            try {
                info = getSnapshot(REPO, snapshotName);
            } catch (SnapshotMissingException e) {
                throw new AssertionError("snapshot not yet visible in the repository", e);
            }
            assertThat(info.state(), equalTo(SnapshotState.SUCCESS));
        }, 60, TimeUnit.SECONDS);
        return getSnapshot(REPO, snapshotName);
    }

    private String getPolicyApiResponse() throws Exception {
        GetSnapshotLifecycleAction.Response response = client().execute(
            GetSnapshotLifecycleAction.INSTANCE,
            new GetSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, POLICY_ID)
        ).get();
        return Strings.toString(response);
    }

    private SnapshotLifecyclePolicyMetadata storedPolicyMetadata() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        SnapshotLifecycleMetadata metadata = clusterService.state()
            .metadata()
            .getProject()
            .custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        return metadata.getSnapshotConfigurations().get(POLICY_ID);
    }

    private SnapshotLifecyclePolicy storedPolicy() {
        SnapshotLifecyclePolicyMetadata policyMetadata = storedPolicyMetadata();
        assertThat(policyMetadata, notNullValue());
        return policyMetadata.getPolicy();
    }

    private void deletePolicyAndIndex() throws Exception {
        assertAcked(
            client().execute(
                DeleteSnapshotLifecycleAction.INSTANCE,
                new DeleteSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, POLICY_ID)
            ).get()
        );
        assertAcked(indicesAdmin().prepareDelete(INDEX).get());
    }

    private org.elasticsearch.action.ActionFuture<RestoreSnapshotResponse> restore(String snapshotName, String encryptedDataPassword) {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, snapshotName).waitForCompletion(true)
            .includeGlobalState(true);
        if (encryptedDataPassword != null) {
            request.encryptedData(new SnapshotEncryptedData(new SecureString(encryptedDataPassword.toCharArray()), null));
        }
        return client().execute(TransportRestoreSnapshotAction.TYPE, request);
    }
}
