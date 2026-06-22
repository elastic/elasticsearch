/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

// 3 master-eligible nodes so testKeySurvivesMasterFailover keeps a quorum after stopping one
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class ProjectEncryptionKeyIT extends SecurityIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";

    @Before
    public void checkFeatureFlag() {
        assumeTrue(
            "project encryption key feature flag must be enabled",
            ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // The encryption settings are only registered when the feature flag is enabled
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()) {
            SecuritySettingsSource.addSecureSettings(builder, secure -> {
                secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
                secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD);
            });
        }
        return builder.build();
    }

    private ProjectEncryptionKeyMetadata getKeyFromClusterState(String nodeName) {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    public void testKeyIsGeneratedOnFreshCluster() throws Exception {
        ensureGreen();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat("Project encryption key should be generated", pek, notNullValue());
            assertNotNull(pek.getActiveKeyId());
            assertEquals(PASSWORD_ID, pek.getPasswordId());
            EncryptedData encryptedActive = pek.getEncryptedKey(pek.getActiveKeyId());
            assertThat(encryptedActive, notNullValue());
            // Canonical wrap layout: [kdf_version(1) | salt(16) | AesGcm output(version+iv+ciphertext+tag)] over a 32-byte PEK.
            assertThat(
                encryptedActive.payload().length,
                equalTo(
                    PasswordBasedEncryption.SALT_OFFSET + PasswordBasedEncryption.SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES
                        + PasswordBasedEncryption.PEK_LENGTH_BYTES
                )
            );
            assertThat(encryptedActive.keyId(), equalTo(PASSWORD_ID));
        });
    }

    public void testAllNodesHaveSameKey() throws Exception {
        ensureGreen();

        assertBusy(() -> {
            String firstKeyId = null;
            for (String nodeName : internalCluster().getNodeNames()) {
                ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(nodeName);
                assertThat("Key should be available on node " + nodeName, pek, notNullValue());
                if (firstKeyId == null) {
                    firstKeyId = pek.getActiveKeyId();
                } else {
                    assertEquals("All nodes should have the same active key", firstKeyId, pek.getActiveKeyId());
                }
            }
        });
    }

    public void testKeySurvivesMasterFailover() throws Exception {
        ensureGreen();

        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        ProjectEncryptionKeyMetadata original = getKeyFromClusterState(internalCluster().getMasterName());

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat(pek, notNullValue());
            assertEquals("Key ID should be the same after master failover", original.getActiveKeyId(), pek.getActiveKeyId());
        });
    }

    public void testKeySurvivesFullClusterRestart() throws Exception {
        ensureGreen();

        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        ProjectEncryptionKeyMetadata original = getKeyFromClusterState(internalCluster().getMasterName());

        internalCluster().fullRestart();
        ensureGreen();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat("Key should survive full cluster restart", pek, notNullValue());
            assertEquals("Key ID should be the same after restart", original.getActiveKeyId(), pek.getActiveKeyId());
        });
    }

    public void testWrappedKeyBytesRedactedFromClusterStateApi() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
        ObjectPath response = ObjectPath.createFromResponse(performClusterStateRequest());

        String typePath = "metadata." + ProjectEncryptionKeyMetadata.TYPE;
        assertEquals(pek.getActiveKeyId(), response.evaluate(typePath + ".active_key_id"));
        assertEquals(pek.getPasswordId(), response.evaluate(typePath + ".password_id"));
        for (String keyId : pek.getKeys().keySet()) {
            assertNotNull(
                "generated_at must be visible for key " + keyId,
                response.evaluate(typePath + ".keys." + keyId + ".generated_at")
            );
            assertNull(
                "wrapped key bytes must not be exposed via the cluster state API for key " + keyId,
                response.evaluate(typePath + ".keys." + keyId + ".bytes")
            );
        }
    }

    public void testResetWithoutAcceptDataLossIsRejected() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        Request request = new Request("POST", "/_encryption/_reset");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testResetRemovesProjectEncryptionKeyMetadata() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        performReset();

        // The cluster state update is acknowledged before the REST response returns, so the PEK is already removed.
        assertNull(getKeyFromClusterState(internalCluster().getMasterName()));
    }

    public void testResetTriggersNewKeyGeneration() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));
        String originalKeyId = getKeyFromClusterState(internalCluster().getMasterName()).getActiveKeyId();

        performReset();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat(pek, notNullValue());
            assertThat("reset should produce a new key ID", pek.getActiveKeyId(), not(equalTo(originalKeyId)));
        }, 30, TimeUnit.SECONDS);
    }

    private void performReset() throws IOException {
        Request request = new Request("POST", "/_encryption/_reset");
        request.addParameter("accept_data_loss", "true");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        getRestClient().performRequest(request);
    }

    private Response performClusterStateRequest() throws IOException {
        Request request = new Request("GET", "/_cluster/state/metadata");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        return getRestClient().performRequest(request);
    }
}
